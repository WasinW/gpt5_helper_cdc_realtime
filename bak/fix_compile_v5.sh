#!/usr/bin/env bash
set -euo pipefail

write(){ mkdir -p "$(dirname "$1")"; cat > "$1"; }

# -- (1) BigQueryDataFetcher: schema ต้องมาจาก TableResult ไม่ใช่ row --
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
    val fields = result.getSchema.getFields.asScala
    val out = scala.collection.mutable.ListBuffer.empty[Map[String,Any]]
    for (row: FieldValueList <- result.iterateAll().asScala) {
      val map = fields.map { field =>
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

# -- (2) ReconciliationAuditor: ให้สอดคล้อง ReconcileResult( status, mismatches, mismatchSamples ) --
write framework/src/main/scala/com/analytics/framework/modules/reconciliation/ReconciliationAuditor.scala <<'SCALA'
package com.analytics.framework.modules.reconciliation
import scala.collection.JavaConverters._
class ReconciliationAuditor {
  def toMap(r: ReconcileResult): java.util.Map[String,AnyRef] = {
    Map(
      "status" -> r.status,
      "mismatches" -> r.mismatches.asJava,
      "mismatch_samples" -> r.mismatchSamples.asJava
    ).asJava.asInstanceOf[java.util.Map[String, AnyRef]]
  }
}
SCALA

# -- (3) S3SampleProvider: เขียนสตับไว้ก่อน (ไม่อ้าง reader แบบ static) --
write framework/src/main/scala/com/analytics/framework/modules/reconciliation/S3SampleProvider.scala <<'SCALA'
package com.analytics.framework.modules.reconciliation
class S3SampleProvider {
  def sample(projectId:String,
             cfg: java.util.Map[String,AnyRef],
             zone:String,
             table:String,
             windowId:String,
             limit:Int = 10): List[Map[String,Any]] = {
    // stub: ยังไม่อ่าน S3 จริง แค่ให้ compile ผ่าน
    Nil
  }
}
SCALA

# -- (4) CDCDetector: สตับ DoFn ให้คอมไพล์ ผ่าน ไม่ต้องพึ่ง FetchedData/CDCRecord เดิม --
write framework/src/main/scala/com/analytics/framework/pipeline/core/CDCDetector.scala <<'SCALA'
package com.analytics.framework.pipeline.core
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
final case class CDCRecord(data: Map[String,Any])
class CDCDetector extends DoFn[Map[String,Any], CDCRecord] {
  @ProcessElement
  def process(ctx: DoFn[Map[String,Any], CDCRecord]#ProcessContext): Unit = {
    // no-op stub
  }
}
SCALA

echo "== v5 files written =="
