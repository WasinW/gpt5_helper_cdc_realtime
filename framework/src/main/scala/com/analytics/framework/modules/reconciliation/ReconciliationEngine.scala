package com.analytics.framework.modules.reconciliation

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, Setup}
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, IntervalWindow}
import com.analytics.framework.pipeline.core.CDCRecord
import scala.jdk.CollectionConverters._
import java.time.{Instant => JInstant}

final case class ReconcileResult(
  recordId: String,
  tableName: String,
  zone: String,
  gcpCount: Long,
  awsCount: Long,
  countMatch: Boolean,
  mismatchSamples: List[Map[String, Any]],
  windowStart: String,
  windowEnd: String,
  ts: Long
)

class ReconciliationEngine(
  bucket: String,
  prefix: String,
  idColumn: String,
  tableMappings: Map[String, Map[String, String]],
  sampleLimit: Int = 5
) extends DoFn[CDCRecord, CDCRecord] {

  @transient private var s3: S3SnapshotReader = _
  @transient private var mapper: SchemaMapper = _

  @Setup def setup(): Unit = {
    s3 = new S3SnapshotReader(bucket, prefix)
    s3.init()
    mapper = new SchemaMapper(tableMappings)
  }

  @ProcessElement
  def process(ctx: DoFn[CDCRecord, CDCRecord]#ProcessContext, window: BoundedWindow): Unit = {
    val w = window.asInstanceOf[IntervalWindow]
    val rec = ctx.element()
    val rid = rec.recordId
    val table = rec.tableName

    val keys = s3.listKeys(limit = 32).filter(_.nonEmpty)
    val awsRows: List[Map[String, Any]] =
      keys.iterator.flatMap { k =>
        s3.readObjectLines(k).take(2000).flatMap { line =>
          // Expect JSONL; adapt as needed
          import io.circe.parser._
          parse(line).toOption.flatMap(_.asObject).map(_.toMap.map {
            case (k,v) => k -> (if(v.isNull) null else v.toString)
          }).filter(_.getOrElse(idColumn, "") == rid)
        }
      }.take(sampleLimit).toList

    val mappedAwsRows = awsRows.map(r => mapper.mapAwsToGcp(table, r))
    val gcpData = rec.data.asScala.toMap

    val gcpCount = 1L
    val awsCount = mappedAwsRows.size.toLong
    val countMatch = gcpCount == awsCount

    val diffs =
      mappedAwsRows.take(sampleLimit).flatMap { awsRow =>
        val cols = (gcpData.keySet ++ awsRow.keySet).filterNot(_.startsWith("_"))
        val bad = cols.filter { c =>
          val gv = Option(gcpData.getOrElse(c, null)).map(_.toString).orNull
          val av = Option(awsRow.getOrElse(c, null)).map(_.toString).orNull
          gv != av
        }
        if (bad.nonEmpty) Some(Map("record_id" -> rid, "columns" -> bad.mkString(","))) else None
      }

    // (Module 04) send ReconcileResult to BQ
    // println(s"Recon: table=$table zone=${rec.zone} rid=$rid gcp=$gcpCount aws=$awsCount match=$countMatch diffs=$diffs")
    ctx.output(rec)
  }
}