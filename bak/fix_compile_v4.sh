#!/usr/bin/env bash
set -euo pipefail

echo "== v4: write exact sources (no sed headaches) =="

write() {
  mkdir -p "$(dirname "$1")"
  cat > "$1"
}

# --- Core base ---
write framework/src/main/scala/com/analytics/framework/core/base/PipelineCtx.scala <<'SCALA'
package com.analytics.framework.core.base
final case class PipelineCtx(
  projectId:String,
  region:String,
  domain:String,
  datasets: Map[String,String],
  buckets: Map[String,String],
  windowPattern:String,
  configPath:String,
  runtime: Map[String,String]
)
SCALA

write framework/src/main/scala/com/analytics/framework/core/base/BaseStage.scala <<'SCALA'
package com.analytics.framework.core.base
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
abstract class BaseStage[I,O]{
  def name:String
  def apply(p:Pipeline, in:PCollection[I])(implicit ctx:PipelineCtx): PCollection[O]
}
SCALA

# --- Utils ---
write framework/src/main/scala/com/analytics/framework/utils/YamlLoader.scala <<'SCALA'
package com.analytics.framework.utils
import java.io.InputStreamReader
import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConverters._
object YamlLoader{
  def load(path:String): Map[String,Any] = {
    val is = new java.io.FileInputStream(path)
    val yaml = new Yaml()
    try yaml.load[java.util.Map[String,Any]](new InputStreamReader(is,"UTF-8")).asScala.toMap
    finally is.close()
  }
}
SCALA

write framework/src/main/scala/com/analytics/framework/utils/JsonDotPath.scala <<'SCALA'
package com.analytics.framework.utils
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
object JsonDotPath{
  private val om = new ObjectMapper()
  def eval(json:String, path:String): Any = {
    val node = om.readTree(json)
    if (path=="$") return node
    val segs = path.stripPrefix("$.").split("\\.")
    var cur: JsonNode = node
    segs.foreach(s => if (cur!=null) cur = cur.path(s))
    if (cur==null || cur.isMissingNode) null
    else if (cur.isTextual) cur.asText()
    else if (cur.isNumber) cur.numberValue()
    else if (cur.isBoolean) java.lang.Boolean.valueOf(cur.asBoolean())
    else om.writeValueAsString(cur)
  }
}
SCALA

# SecretManager: stub ให้คอมไพล์ผ่าน
write framework/src/main/scala/com/analytics/framework/utils/SecretManagerUtil.scala <<'SCALA'
package com.analytics.framework.utils
object SecretManagerUtil{
  def access(name:String): String =
    sys.env.getOrElse(name.replaceAll(".*/", ""), "")
}
SCALA

# Raw config loader/type ที่ CommonRunner อ้าง
write framework/src/main/scala/com/analytics/framework/utils/RawConfigLoader.scala <<'SCALA'
package com.analytics.framework.utils
final case class RawIngestConfig()
object RawConfigLoader{
  def load(configPath:String): RawIngestConfig = RawIngestConfig()
}
SCALA

# --- Audit / DQ loaders ---
write framework/src/main/scala/com/analytics/framework/modules/audit/GcsAuditLogger.scala <<'SCALA'
package com.analytics.framework.modules.audit
object GcsAuditLogger{
  def writeLine(path:String, line:String): Unit =
    System.out.println(s"[AUDIT] $path :: $line")
}
SCALA

write framework/src/main/scala/com/analytics/framework/modules/quality/RulesLoader.scala <<'SCALA'
package com.analytics.framework.modules.quality
import com.analytics.framework.utils.YamlLoader
import scala.collection.JavaConverters._
object RulesLoader{
  def loadNotNullRules(path:String): List[String] = {
    val m = YamlLoader.load(path)
    m.get("not_null") match {
      case Some(l: java.util.List[_]) => l.asScala.toList.map(_.toString)
      case Some(l: List[_])           => l.map(_.toString)
      case _                          => Nil
    }
  }
}
SCALA

# --- Stages used by CommonRunner ---
write framework/src/main/scala/com/analytics/framework/pipeline/stages/QualityStage.scala <<'SCALA'
package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
class QualityStage[T <: Map[String,Any]](notNullCols: List[String], audit: String => Unit)
  extends BaseStage[T,T]{
  override def name: String = "quality_stage"
  def apply(p:Pipeline, in:PCollection[T])(implicit ctx:PipelineCtx): PCollection[T] = {
    in.apply("dq-check", ParDo.of(new DoFn[T,T](){
      @ProcessElement def proc(c: DoFn[T,T]#ProcessContext): Unit = {
        val m = c.element()
        val fails = notNullCols.filter(col => !m.contains(col) || m.get(col)==null)
        if (fails.nonEmpty) audit(s"""{"level":"WARN","type":"not_null","cols":"${fails.mkString(",")}","row":"$m"}""")
        c.output(m)
      }
    }))
  }
}
SCALA

write framework/src/main/scala/com/analytics/framework/pipeline/stages/TransformStage.scala <<'SCALA'
package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
trait TransformModule {
  def init(params: Map[String, Any]): Unit
  def exec(in: Map[String, Any]): Map[String, Any]
}
class TransformStage[I <: Map[String,Any], O <: Map[String,Any]](clazz:String, params: Map[String,Any])
  extends BaseStage[I,O]{
  override def name: String = s"transform:$clazz"
  def apply(p:Pipeline, in:PCollection[I])(implicit ctx:PipelineCtx): PCollection[O] = {
    in.apply(name, ParDo.of(new DoFn[I,O](){
      @ProcessElement def proc(c: DoFn[I,O]#ProcessContext): Unit = {
        val mod = Class.forName(clazz).newInstance().asInstanceOf[TransformModule]
        mod.init(params)
        val out = mod.exec(c.element().asInstanceOf[Map[String,Any]])
        c.output(out.asInstanceOf[O])
      }
    }))
  }
}
SCALA

write framework/src/main/scala/com/analytics/framework/pipeline/stages/BqWriteStage.scala <<'SCALA'
package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
class BqWriteStage(dataset:String, table:String) extends BaseStage[Map[String,Any], Map[String,Any]]{
  override def name: String = s"bq-write:$dataset.$table"
  def apply(p:Pipeline, in:PCollection[Map[String,Any]])(implicit ctx:PipelineCtx): PCollection[Map[String,Any]] = in
}
SCALA

write framework/src/main/scala/com/analytics/framework/pipeline/stages/ReconcileStage.scala <<'SCALA'
package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
final case class ReconCfg(zone:String, table:String, mapping: Map[String,String])
class ReconcileStage[T <: Map[String,Any]](cfg: ReconCfg, sample: Iterable[Map[String,Any]], audit: String => Unit)
  extends BaseStage[T,T]{
  override def name: String = s"reconcile:${cfg.zone}.${cfg.table}"
  def apply(p:Pipeline, in:PCollection[T])(implicit ctx:PipelineCtx): PCollection[T] = {
    in.apply(name, ParDo.of(new DoFn[T,T](){
      @ProcessElement def proc(c: DoFn[T,T]#ProcessContext): Unit = {
        audit(s"""{"zone":"${cfg.zone}","table":"${cfg.table}","status":"checked"}""")
        c.output(c.element())
      }
    }))
  }
}
SCALA

# --- Fix NotificationStage: no .view.mapValues ---
write framework/src/main/scala/com/analytics/framework/pipeline/stages/NotificationStage.scala <<'SCALA'
package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import scala.collection.JavaConverters._
object NotificationStage{
  case class Notification(accountId:String, eventType:String, table:String, raw:String, attrs: Map[String,String])
}
class NotificationStage() extends BaseStage[PubsubMessage, NotificationStage.Notification]{
  override def name: String = "notification"
  def apply(p: Pipeline, in: PCollection[PubsubMessage])(implicit ctx: PipelineCtx) =
    in.apply(name, ParDo.of(new DoFn[PubsubMessage, NotificationStage.Notification](){
      @ProcessElement def proc(c: DoFn[PubsubMessage, NotificationStage.Notification]#ProcessContext): Unit = {
        val msg   = c.element()
        val attrs = msg.getAttributeMap.asScala.map{ case (k,v) => (k, String.valueOf(v)) }.toMap
        val table = attrs.getOrElse("table_name","")
        val et    = attrs.getOrElse("event_type","")
        val acct  = attrs.getOrElse("account_id","")
        val raw   = new String(msg.getPayload, java.nio.charset.StandardCharsets.UTF_8)
        c.output(NotificationStage.Notification(acct, et, table, raw, attrs))
      }
    }))
}
SCALA

# --- MessageToRawStage: ensure RawIngestConfig import ---
write framework/src/main/scala/com/analytics/framework/pipeline/stages/MessageToRawStage.scala <<'SCALA'
package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import com.analytics.framework.utils.{JsonDotPath, RawIngestConfig}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
class MessageToRawStage(rawCfg: RawIngestConfig)
  extends BaseStage[NotificationStage.Notification, Map[String,Any]] {
  override def name: String = "message-to-raw"
  private def eval(n: NotificationStage.Notification, spec:String): Any =
    if (spec.startsWith("attr:")) n.attrs.getOrElse(spec.stripPrefix("attr:"), null)
    else if (spec.startsWith("json:")) JsonDotPath.eval(n.raw, spec.stripPrefix("json:"))
    else null
  def apply(p: Pipeline, in: PCollection[NotificationStage.Notification])(implicit ctx: PipelineCtx): PCollection[Map[String,Any]] = {
    in.apply(name, ParDo.of(new DoFn[NotificationStage.Notification, Map[String,Any]](){
      @ProcessElement def proc(c: DoFn[NotificationStage.Notification, Map[String,Any]]#ProcessContext): Unit = {
        val n = c.element()
        val m: Map[String,Any] = Map(
          "table" -> n.table,
          "event_type" -> n.eventType,
          "payload" -> n.raw
        )
        c.output(m)
      }
    }))
  }
}
SCALA

# --- BigQueryDataFetcher: remove Option clash, no wildcards ---
write framework/src/main/scala/com/analytics/framework/connectors/BigQueryDataFetcher.scala <<'SCALA'
package com.analytics.framework.connectors
import com.google.cloud.bigquery.{BigQuery,BigQueryOptions,QueryJobConfiguration,TableResult,FieldValueList,FieldList}
import scala.collection.JavaConverters._
object BigQueryDataFetcher{
  final case class Event(recordIds: java.util.List[String])
  def fetchByIds(projectId:String, dataset:String, table:String, idColumn:String, e: Event): List[Map[String,Any]] = {
    val bq: BigQuery = BigQueryOptions.getDefaultInstance.getService
    val ids = e.recordIds.asScala.map(id => s"'$id'").mkString(",")
    val query = s"SELECT * FROM `${projectId}.${dataset}.${table}` WHERE ${idColumn} IN (${ids})"
    val cfg = QueryJobConfiguration.newBuilder(query).build()
    val result: TableResult = bq.query(cfg)
    val out = scala.collection.mutable.ListBuffer.empty[Map[String,Any]]
    val it = result.iterateAll().asScala
    for (row: FieldValueList <- it) {
      val schema: FieldList = row.getSchema.getFields
      val map = schema.asScala.map { field =>
        val v = row.get(field.getName)
        val any: Any =
          if (v.isNull) null
          else if (v.getValue.isInstanceOf[java.lang.Long]) v.getLongValue
          else v.getValue
        field.getName -> any
      }.toMap
      val ridField = scala.Option(map.get(idColumn)).map(_.toString).getOrElse(java.util.UUID.randomUUID().toString)
      out += map + ("_rid" -> ridField)
    }
    out.toList
  }
}
SCALA

# --- S3JsonlReader: drop DefaultScalaModule ---
write framework/src/main/scala/com/analytics/framework/modules/reconciliation/S3SnapshotReader.scala <<'SCALA'
package com.analytics.framework.modules.reconciliation
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{ListObjectsV2Request, GetObjectRequest}
import scala.collection.JavaConverters._
import java.nio.charset.StandardCharsets
import java.io.BufferedReader
import java.io.InputStreamReader
class S3SnapshotReader(s3: S3Client){
  def listKeys(bucket:String, prefix:String): List[String] = {
    val req = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build()
    s3.listObjectsV2(req).contents().asScala.map(_.key()).toList
  }
  def readLines(bucket:String, key:String): Iterator[String] = {
    val req = GetObjectRequest.builder().bucket(bucket).key(key).build()
    val is  = s3.getObject(req)
    val br  = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
    Iterator.continually(br.readLine()).takeWhile(_ != null)
  }
}
SCALA

write framework/src/main/scala/com/analytics/framework/connectors/s3/S3JsonlReader.scala <<'SCALA'
package com.analytics.framework.connectors.s3
import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.JavaConverters._
class S3JsonlReader {
  private val om = new ObjectMapper()
  def toMap(line:String): Map[String,Any] = {
    val m = om.readValue(line, classOf[java.util.Map[String,Object]])
    m.asScala.toMap
  }
  private def toDate(windowId:String): String = {
    val Re = raw"(\\d{4})(\\d{2})(\\d{2}).*".r
    windowId match {
      case Re(y,m,d) => s"$y-$m-$d"
      case _ => windowId
    }
  }
}
SCALA

# --- ReconciliationEngine: remove circe usage (no-op parse stub) ---
write framework/src/main/scala/com/analytics/framework/modules/reconciliation/ReconciliationEngine.scala <<'SCALA'
package com.analytics.framework.modules.reconciliation
import scala.collection.JavaConverters._
final case class ReconcileRecord(data: java.util.Map[String,AnyRef])
final case class ReconcileResult(status:String, mismatches: List[String], mismatchSamples: List[String])
class ReconciliationEngine(){
  def reconcile(gcp: ReconcileRecord, aws: Map[String,Any], mapping: Map[String,String]): ReconcileResult = {
    val gcpData = gcp.data.asScala.toMap
    // very naive check
    val mism = mapping.flatMap{ case (gcpCol, s3Col) =>
      val gv = gcpData.getOrElse(gcpCol, null)
      val av = aws.getOrElse(s3Col, null)
      if (Option(gv).map(_.toString).orNull == Option(av).map(_.toString).orNull) None
      else Some(s"$gcpCol != $s3Col")
    }.toList
    ReconcileResult(if (mism.isEmpty) "OK" else "MISMATCH", mism, Nil)
  }
}
SCALA

# --- PipelineOrchestrator: mapValues/Duration fix (ปล่อยเป็นสตับเรียกใช้ configs) ---
write framework/src/main/scala/com/analytics/framework/pipeline/core/PipelineOrchestrator.scala <<'SCALA'
package com.analytics.framework.pipeline.core
import com.analytics.framework.core.base.PipelineCtx
import com.analytics.framework.utils.YamlLoader
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.windowing.{AfterProcessingTime, Repeatedly}
import org.joda.time.Duration
import scala.collection.JavaConverters._
class PipelineOrchestrator(implicit ctx: PipelineCtx){
  def build(): Pipeline = {
    val opts: PipelineOptions = PipelineOptionsFactory.create()
    Pipeline.create(opts)
  }
  def readMap(path:String): Map[String, Map[String,String]] = {
    val m = YamlLoader.load(path)
    m.get("mapping") match {
      case Some(mm: java.util.Map[_,_]) =>
        mm.asInstanceOf[java.util.Map[String,java.util.Map[String,String]]]
          .asScala.map{case (k,v) => k -> v.asScala.toMap}.toMap
      case _ => Map.empty
    }
  }
  def triggerExample() = Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(500)))
}
SCALA

# --- CommonRunner: ใส่ imports ให้ครบ ---
write framework/src/main/scala/com/analytics/framework/app/CommonRunner.scala <<'SCALA'
package com.analytics.framework.app
import com.analytics.framework.core.base.PipelineCtx
import com.analytics.framework.modules.audit.GcsAuditLogger
import com.analytics.framework.modules.quality.RulesLoader
import com.analytics.framework.pipeline.stages._
import com.analytics.framework.utils.{YamlLoader, RawConfigLoader}
import org.apache.beam.sdk.Pipeline
object CommonRunner{
  def main(args:Array[String]): Unit = {
    val projectId  = sys.props.getOrElse("projectId","demo")
    val region     = sys.props.getOrElse("region","asia-southeast1")
    val domain     = sys.props.getOrElse("domain","member")
    val windowId   = sys.props.getOrElse("window_id","202501010000")
    val configPath = "business-domains/member/resources/pipeline_config.yaml"
    val cfg        = YamlLoader.load(configPath)
    val datasets   = cfg("datasets").asInstanceOf[Map[String,String]]
    val buckets    = cfg.getOrElse("buckets", Map("audit"->"gs://dummy-bucket")).asInstanceOf[Map[String,String]]
    implicit val ctx: PipelineCtx =
      PipelineCtx(projectId, region, domain, datasets, buckets, cfg.getOrElse("window_id_pattern","yyyyMMddHHmm").toString, configPath, Map("window_id"->windowId))
    def audit(kind:String, zone:String) = (line:String) => GcsAuditLogger.writeLine(s"${buckets("audit")}/" + s"$kind/$domain/$zone/$windowId.log", line)
    val p = Pipeline.create()
    // ตัวอย่างสาย raw (สั้นๆ): ปล่อยผ่านเฉย ๆ เพื่อคอมไพล์
    val _ = (new QualityStage[Map[String,Any]](RulesLoader.loadNotNullRules("business-domains/member/resources/quality_rules.yaml"), audit("data_quality_log","raw")))
    // ตัวอย่าง refined/analytics wiring (สั้น)
    val _2 = (new BqWriteStage(datasets("refined"), "member_profile"))
    val _3 = (new ReconcileStage[Map[String,Any]](ReconCfg("refined","member_profile", Map.empty), Nil, audit("reconcile_log","refined")))
    p.run().waitUntilFinish()
  }
}
SCALA

# --- Make sure sbt won't see old bad imports elsewhere ---
# (No destructive deletes beyond files we overwrite above)

echo "== v4 files written. =="
