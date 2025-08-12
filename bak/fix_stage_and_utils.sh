#!/usr/bin/env bash
set -euo pipefail

# Root directory of the git repository
ROOT_DIR="$(git rev-parse --show-toplevel)"

echo "Applying fixes for stage classes and utility configuration..."

# Helper to backup a file if it exists
backup_if_exists() {
  local file="$1"
  if [[ -f "$file" ]]; then
    local ts=$(date +%s)
    cp "$file" "${file}.bak.${ts}"
    echo "Backed up $file -> ${file}.bak.${ts}"
  fi
}

# Write RawIngestConfig.scala
RAW_CFG_PATH="$ROOT_DIR/framework/src/main/scala/com/analytics/framework/utils/RawIngestConfig.scala"
backup_if_exists "$RAW_CFG_PATH"
mkdir -p "$(dirname "$RAW_CFG_PATH")"
cat > "$RAW_CFG_PATH" <<'SCALA'
package com.analytics.framework.utils

/**
 * Configuration for raw ingestion mappings.
 *
 * A `RawIngestConfig` defines how to extract fields from an incoming message
 * when converting it into a raw record.  Each entry in the `fields` list
 * specifies a target column name and a specification describing how to
 * obtain the value from the message.  Specifications are interpreted by
 * `MessageToRawStage` and may begin with "attr:" to read from the
 * Pub/Sub message attributes or "json:" to read a dot‑path from the
 * JSON payload.
 *
 * @param idField optional name of the message attribute or JSON field
 *                that uniquely identifies the record
 * @param tableField optional name of the message attribute or JSON field
 *                   that contains the destination table name
 * @param fields list of (targetColumn, specification) pairs
 */
final case class RawIngestConfig(
    idField: Option[String] = None,
    tableField: Option[String] = None,
    fields: List[(String, String)] = List.empty
)
SCALA

# Write BqReadStage.scala
BQ_READ_PATH="$ROOT_DIR/framework/src/main/scala/com/analytics/framework/pipeline/stages/BqReadStage.scala"
backup_if_exists "$BQ_READ_PATH"
mkdir -p "$(dirname "$BQ_READ_PATH")"
cat > "$BQ_READ_PATH" <<'SCALA'
package com.analytics.framework.pipeline.stages

import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO => BQ}
import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

/**
 * Stage that reads rows from a BigQuery table and converts them into a
 * collection of Scala maps.  Each element in the resulting PCollection is a
 * Map[String, Any] keyed by column name.  The input PCollection is ignored
 * because this is a source stage.
 *
 * @param dataset BigQuery dataset name (without project prefix)
 * @param table   BigQuery table name
 */
class BqReadStage(dataset: String, table: String)
    extends BaseStage[Any, Map[String, Any]] {
  override def name: String = s"BqRead($dataset.$table)"

  /**
   * Read from BigQuery and map TableRow objects into Scala maps.
   *
   * @param p   the Beam pipeline
   * @param in  unused input collection
   * @param ctx implicit pipeline context providing the project ID
   * @return a PCollection of maps representing BigQuery rows
   */
  override def apply(p: Pipeline, in: PCollection[Any])(implicit ctx: PipelineCtx): PCollection[Map[String, Any]] = {
    val rows = p.apply(name, BQ.readTableRows().from(s"${ctx.projectId}:$dataset.$table"))
    rows.apply("RowToMap", ParDo.of(new DoFn[TableRow, Map[String, Any]]() {
      @ProcessElement
      def proc(c: DoFn[TableRow, Map[String, Any]]#ProcessContext): Unit = {
        val tr = c.element()
        import scala.collection.JavaConverters._
        val m = tr.keySet.asScala.map(k => k -> tr.get(k)).toMap
        c.output(m)
      }
    }))
  }
}
SCALA

# Write BqWriteDynamicStage.scala
BQ_WRITE_DYN_PATH="$ROOT_DIR/framework/src/main/scala/com/analytics/framework/pipeline/stages/BqWriteDynamicStage.scala"
backup_if_exists "$BQ_WRITE_DYN_PATH"
mkdir -p "$(dirname "$BQ_WRITE_DYN_PATH")"
cat > "$BQ_WRITE_DYN_PATH" <<'SCALA'
package com.analytics.framework.pipeline.stages

import com.google.api.services.bigquery.model.{TableRow, TableSchema}
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, DynamicDestinations, TableDestination}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.{PCollection, ValueInSingleWindow}
import org.apache.beam.sdk.Pipeline

import com.analytics.framework.core.base.{BaseStage, PipelineCtx}

/**
 * Stage that writes a stream of maps into BigQuery using dynamic table
 * destinations.  Each input map must contain a "table" key which specifies
 * the destination table name; if the key is missing the record will be
 * written to a table named "unknown".  The write is performed as a
 * side‑effect; the original input PCollection is passed through unchanged.
 *
 * @param dataset the BigQuery dataset where tables reside
 */
class BqWriteDynamicStage(dataset: String)
    extends BaseStage[Map[String, Any], Map[String, Any]] {
  override def name: String = s"BqWriteDynamic($dataset)"

  /**
   * Writes the input maps to BigQuery tables determined at runtime.
   *
   * @param p   the Beam pipeline
   * @param in  the input collection of maps (must include a "table" key)
   * @param ctx implicit pipeline context providing the project ID
   * @return the input collection unchanged
   */
  override def apply(p: Pipeline, in: PCollection[Map[String, Any]])(implicit ctx: PipelineCtx): PCollection[Map[String, Any]] = {
    val project = ctx.projectId
    val ds = dataset
    // Map Scala Map -> TableRow (must have key "table" for choosing destination)
    val tableRows = in.apply("MapToTableRow", ParDo.of(new DoFn[Map[String, Any], TableRow] {
      @ProcessElement
      def proc(c: DoFn[Map[String, Any], TableRow]#ProcessContext): Unit = {
        val m = c.element()
        val tr = new TableRow()
        m.foreach { case (k, v) => tr.set(k, if (v == null) null else v.toString) }
        if (!m.contains("table")) tr.set("table", "unknown")
        c.output(tr)
      }
    }))
    // Write to BigQuery using DynamicDestinations
    tableRows.apply("bqDynamicWrite",
      BigQueryIO.writeTableRows()
        .to(new DynamicDestinations[TableRow, String]() {
          override def getDestination(e: ValueInSingleWindow[TableRow]): String = {
            val tr = e.getValue
            val table = Option(tr.get("table")).map(_.toString).getOrElse("unknown")
            s"$project:$ds.$table"
          }
          override def getTable(dest: String): TableDestination =
            new TableDestination(dest, s"dynamic to $dest")
          override def getSchema(dest: String): TableSchema = null // CREATE_NEVER → no schema
        })
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
    )
    // Pass through the original collection
    in
  }
}
SCALA

# Write FieldMapStage.scala
FIELD_MAP_PATH="$ROOT_DIR/framework/src/main/scala/com/analytics/framework/pipeline/stages/FieldMapStage.scala"
backup_if_exists "$FIELD_MAP_PATH"
mkdir -p "$(dirname "$FIELD_MAP_PATH")"
cat > "$FIELD_MAP_PATH" <<'SCALA'
package com.analytics.framework.pipeline.stages

import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

/**
 * Stage that remaps fields of an input map according to a provided mapping.
 * Each entry in the mapping defines a destination field name and the source
 * field name to copy from the input map.  Missing values are replaced with
 * null.
 *
 * @param mapping map of destination field names to source field names
 */
class FieldMapStage(mapping: Map[String, String])
    extends BaseStage[Map[String, Any], Map[String, Any]] {
  override def name: String = "FieldMap"

  /**
   * Apply the field mapping to each record in the input PCollection.
   *
   * @param p   the Beam pipeline
   * @param in  input collection of maps
   * @param ctx implicit pipeline context (unused)
   * @return a PCollection of remapped maps
   */
  override def apply(p: Pipeline, in: PCollection[Map[String, Any]])(implicit ctx: PipelineCtx): PCollection[Map[String, Any]] = {
    in.apply(name, ParDo.of(new DoFn[Map[String, Any], Map[String, Any]]() {
      @ProcessElement
      def proc(c: DoFn[Map[String, Any], Map[String, Any]]#ProcessContext): Unit = {
        val row = c.element()
        val out = mapping.map { case (dst, src) => dst -> row.getOrElse(src, null) }
        c.output(out)
      }
    }))
  }
}
SCALA

# Write FilterByTableStage.scala
FILTER_BY_TABLE_PATH="$ROOT_DIR/framework/src/main/scala/com/analytics/framework/pipeline/stages/FilterByTableStage.scala"
backup_if_exists "$FILTER_BY_TABLE_PATH"
mkdir -p "$(dirname "$FILTER_BY_TABLE_PATH")"
cat > "$FILTER_BY_TABLE_PATH" <<'SCALA'
package com.analytics.framework.pipeline.stages

import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

/**
 * Stage that filters records based on the value of their "table" field.
 * Only records whose "table" field matches the configured table name are
 * passed through.
 *
 * @param table the table name to filter on
 */
class FilterByTableStage(table: String)
    extends BaseStage[Map[String, Any], Map[String, Any]] {
  override def name: String = s"FilterByTable($table)"

  /**
   * Filter the input collection to records whose table field matches.
   *
   * @param p   the Beam pipeline
   * @param in  input collection of maps
   * @param ctx implicit pipeline context (unused)
   * @return a PCollection containing only maps with the desired table name
   */
  override def apply(p: Pipeline, in: PCollection[Map[String, Any]])(implicit ctx: PipelineCtx): PCollection[Map[String, Any]] = {
    in.apply(name, ParDo.of(new DoFn[Map[String, Any], Map[String, Any]]() {
      @ProcessElement
      def proc(c: DoFn[Map[String, Any], Map[String, Any]]#ProcessContext): Unit = {
        val e = c.element()
        if (e.getOrElse("table", "") == table) c.output(e)
      }
    }))
  }
}
SCALA

# Write NotificationStage.scala
NOTIF_PATH="$ROOT_DIR/framework/src/main/scala/com/analytics/framework/pipeline/stages/NotificationStage.scala"
backup_if_exists "$NOTIF_PATH"
mkdir -p "$(dirname "$NOTIF_PATH")"
cat > "$NOTIF_PATH" <<'SCALA'
package com.analytics.framework.pipeline.stages

import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import scala.collection.JavaConverters._

/**
 * Stage that converts Pub/Sub messages into `Notification` records.  A
 * `Notification` contains common attributes such as accountId, eventType,
 * table and the raw JSON payload.
 */
object NotificationStage {
  /**
   * Parsed representation of a Pub/Sub message used by downstream stages.
   *
   * @param accountId the account identifier associated with the notification
   * @param eventType the type of event (e.g. INSERT, UPDATE, DELETE)
   * @param table     the table affected by the event
   * @param raw       the raw JSON payload
   * @param attrs     all message attributes as a Scala map
   */
  case class Notification(
      accountId: String,
      eventType: String,
      table: String,
      raw: String,
      attrs: Map[String, String]
  )
}

/**
 * Stage that decodes incoming Pub/Sub messages into Notification objects.
 */
class NotificationStage() extends BaseStage[PubsubMessage, NotificationStage.Notification] {
  override def name: String = "notification"

  /**
   * Extract attributes and payload from each Pub/Sub message.
   *
   * @param p   the Beam pipeline
   * @param in  input collection of PubsubMessage
   * @param ctx implicit pipeline context (unused)
   * @return a PCollection of `Notification` objects
   */
  override def apply(p: Pipeline, in: PCollection[PubsubMessage])(implicit ctx: PipelineCtx): PCollection[NotificationStage.Notification] = {
    in.apply(name, ParDo.of(new DoFn[PubsubMessage, NotificationStage.Notification]() {
      @ProcessElement
      def proc(c: DoFn[PubsubMessage, NotificationStage.Notification]#ProcessContext): Unit = {
        val msg = c.element()
        val attrs = msg.getAttributeMap.asScala.map { case (k, v) => (k, String.valueOf(v)) }.toMap
        val table = attrs.getOrElse("table_name", "")
        val et = attrs.getOrElse("event_type", "")
        val acct = attrs.getOrElse("account_id", "")
        val raw = new String(msg.getPayload, java.nio.charset.StandardCharsets.UTF_8)
        c.output(NotificationStage.Notification(acct, et, table, raw, attrs))
      }
    }))
  }
}
SCALA

# Write MessageToRawStage.scala
MSG_TO_RAW_PATH="$ROOT_DIR/framework/src/main/scala/com/analytics/framework/pipeline/stages/MessageToRawStage.scala"
backup_if_exists "$MSG_TO_RAW_PATH"
mkdir -p "$(dirname "$MSG_TO_RAW_PATH")"
cat > "$MSG_TO_RAW_PATH" <<'SCALA'
package com.analytics.framework.pipeline.stages

import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import com.analytics.framework.utils.{JsonDotPath, RawIngestConfig}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

/**
 * Stage that converts `Notification` objects into raw records represented as
 * maps.  Each output map contains standard fields (table, event_type,
 * payload) and may be extended using the provided `RawIngestConfig`.
 *
 * @param rawCfg configuration describing how to map fields from the notification
 */
class MessageToRawStage(rawCfg: RawIngestConfig)
    extends BaseStage[NotificationStage.Notification, Map[String, Any]] {
  override def name: String = "message-to-raw"

  /**
   * Evaluate a specification against a notification.  Specifications prefixed
   * with `attr:` read from the message attributes, while `json:` prefixes
   * read from the JSON payload using dot‑notation.  Unsupported specs return
   * null.
   */
  private def eval(n: NotificationStage.Notification, spec: String): Any = {
    if (spec.startsWith("attr:")) {
      n.attrs.getOrElse(spec.stripPrefix("attr:"), null)
    } else if (spec.startsWith("json:")) {
      // Use JsonDotPath to extract from the raw JSON payload
      JsonDotPath.eval(n.raw, spec.stripPrefix("json:")).orNull
    } else {
      null
    }
  }

  /**
   * Convert each notification into a raw record map according to the config.
   * Standard fields are always included; additional fields defined by
   * `rawCfg.fields` are appended.
   */
  override def apply(p: Pipeline, in: PCollection[NotificationStage.Notification])(implicit ctx: PipelineCtx): PCollection[Map[String, Any]] = {
    in.apply(name, ParDo.of(new DoFn[NotificationStage.Notification, Map[String, Any]]() {
      @ProcessElement
      def proc(c: DoFn[NotificationStage.Notification, Map[String, Any]]#ProcessContext): Unit = {
        val n = c.element()
        // Base fields
        var m: Map[String, Any] = Map(
          "table" -> n.table,
          "event_type" -> n.eventType,
          "payload" -> n.raw
        )
        // Apply additional field mappings from config
        for ((dst, spec) <- rawCfg.fields) {
          m = m + (dst -> eval(n, spec))
        }
        c.output(m)
      }
    }))
  }
}
SCALA

# Ensure S3JsonlReader has readJsonLines helper
S3_READER_PATH="$ROOT_DIR/framework/src/main/scala/com/analytics/framework/connectors/S3JsonlReader.scala"
if grep -q "def readJsonLines" "$S3_READER_PATH"; then
  echo "S3JsonlReader already has readJsonLines; skipping modification."
else
  echo "Adding readJsonLines helper to S3JsonlReader..."
  backup_if_exists "$S3_READER_PATH"
  cat > "$S3_READER_PATH" <<'SCALA'
package com.analytics.framework.connectors

import scala.io.Source
import java.nio.file.{Files, Paths}
import com.google.gson.{JsonElement, JsonParser}
import scala.collection.JavaConverters._

/**
 * Reader for JSON Lines stored in S3.
 *
 * This stubbed implementation maps S3 URIs to relative filesystem paths by
 * removing the `s3://bucket/` prefix.  It then reads the file line by line,
 * parsing each non‑empty line as JSON via Gson and converting it into a
 * Scala `Map[String, Any]`.  If the file does not exist an empty list is
 * returned.
 */
object S3JsonlReader {
  /**
   * Read a JSON Lines file from an S3 URI.
   *
   * @param uri S3 URI (e.g. "s3://bucket/path/file.jsonl")
   * @return a list of records parsed into Scala maps
   */
  def read(uri: String): List[Map[String, Any]] = {
    // Strip the "s3://bucket/" prefix to map into the local filesystem.  In a
    // real system this would download from S3.
    val localPath = uri.replaceFirst("^s3://[^/]+/", "")
    val p = Paths.get(localPath)
    if (!Files.exists(p)) return Nil
    val src = Source.fromFile(p.toFile)
    try {
      src.getLines().toList.flatMap { line =>
        val trimmed = line.trim
        if (trimmed.isEmpty) Nil
        else {
          try {
            val elem = JsonParser.parseString(trimmed)
            val rec = toScala(elem).asInstanceOf[Map[String, Any]]
            List(rec)
          } catch {
            case _: Exception => Nil
          }
        }
      }
    } finally {
      src.close()
    }
  }

  /**
   * Backwards‑compatibility helper for legacy code.  Some components expect a
   * `readJsonLines` method on the reader that returns a sequence of maps.
   * This method simply delegates to [[read]] and widens the return type
   * to `Seq`.
   *
   * @param uri S3 URI pointing to a JSON Lines file
   * @return a sequence of records parsed into maps
   */
  def readJsonLines(uri: String): Seq[Map[String, Any]] = read(uri)

  /**
   * Recursively convert a Gson [[JsonElement]] into Scala types.  JSON objects
   * become `Map[String, Any]`, arrays become `List[Any]`, primitives become
   * their corresponding Scala values and null becomes null.
   */
  private def toScala(elem: JsonElement): Any = {
    if (elem.isJsonNull) {
      null
    } else if (elem.isJsonPrimitive) {
      val prim = elem.getAsJsonPrimitive
      if (prim.isBoolean) prim.getAsBoolean
      else if (prim.isNumber) prim.getAsNumber
      else prim.getAsString
    } else if (elem.isJsonArray) {
      elem.getAsJsonArray.iterator().asScala.toList.map(e => toScala(e))
    } else if (elem.isJsonObject) {
      elem.getAsJsonObject.entrySet().asScala.map { entry =>
        entry.getKey -> toScala(entry.getValue)
      }.toMap
    } else {
      null
    }
  }
}
SCALA
fi

# Ensure BaseStage has a default run method for backwards compatibility
BASE_STAGE_PATH="$ROOT_DIR/framework/src/main/scala/com/analytics/framework/core/base/BaseStage.scala"
if ! grep -q "def run(" "$BASE_STAGE_PATH"; then
  echo "Adding default run method to BaseStage..."
  backup_if_exists "$BASE_STAGE_PATH"
  # insert the run method before the closing brace of the class
  awk '
    /abstract class BaseStage/ { print; in_class = 1; next }
    in_class && /}/ && !added { 
      print "\n  /**";
      print "   * Process a batch of records using the process method.  This method";
      print "   * provides backwards‑compatibility for older stage implementations that";
      print "   * expect to work with Scala collections via a run method.  The default";
      print "   * implementation simply maps over the input sequence, applying the";
      print "   * process method to each element.  Concrete stages may override this";
      print "   * method to implement more efficient batch processing if desired.";
      print "   *";
      print "   * @param ctx pipeline context";
      print "   * @param in  a sequence of input records";
      print "   * @return a sequence of transformed records";
      print "   */";
      print "  def run(ctx: PipelineCtx, in: Seq[I]): Seq[O] = {";
      print "    in.map { elem =>";
      print "      implicit val ictx: PipelineCtx = ctx";
      print "      process(elem)";
      print "    }";
      print "  }";
      added=1;
    }
    { print }
  ' "$BASE_STAGE_PATH" > "$BASE_STAGE_PATH.tmp" && mv "$BASE_STAGE_PATH.tmp" "$BASE_STAGE_PATH"
fi

echo "Fixes applied. Please run 'sbt -Dsbt.supershell=false clean compile' to verify."