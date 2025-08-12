package com.analytics.framework.modules.audit

import org.apache.beam.sdk.transforms.DoFn
import com.analytics.framework.pipeline.core.CDCRecord
import com.google.cloud.bigquery._
import scala.collection.JavaConverters._

class AuditLogger(projectId: String, frameworkDataset: String, tableName: String, zone: String)
  extends DoFn[CDCRecord, CDCRecord] {

  @transient private var bq: BigQuery = _
  @org.apache.beam.sdk.transforms.DoFn.Setup
  def setup(): Unit = { bq = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService }

  @org.apache.beam.sdk.transforms.DoFn.ProcessElement
  def process(ctx: DoFn[CDCRecord, CDCRecord]#ProcessContext): Unit = {
    val tableId = TableId.of(frameworkDataset, "pipeline_log")
    val row = Map(
      "log_id" -> java.util.UUID.randomUUID().toString,
      "pipeline_name" -> s"${tableName}_${zone}",
      "domain" -> frameworkDataset.replaceAll("_framework$", ""),
      "zone" -> zone,
      "table_name" -> tableName,
      "status" -> "SUCCESS"
    ).asJava.asInstanceOf[java.util.Map[String, AnyRef]]
    bq.insertAll(InsertAllRequest.newBuilder(tableId).addRow(row).build())
    ctx.output(ctx.element())
  }
}