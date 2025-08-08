package com.analytics.framework.pipeline.core

import org.apache.beam.sdk.transforms.DoFn
import com.google.cloud.bigquery._
import scala.jdk.CollectionConverters._

class RawInserter(projectId: String, dataset: String, table: String)
  extends DoFn[CDCRecord, CDCRecord] {

  @transient private var bq: BigQuery = _
  @org.apache.beam.sdk.transforms.DoFn.Setup
  def setup(): Unit = {
    bq = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService
  }

  @org.apache.beam.sdk.transforms.DoFn.ProcessElement
  def process(ctx: DoFn[CDCRecord, CDCRecord]#ProcessContext): Unit = {
    val rec = ctx.element()
    val tableId = TableId.of(dataset, s"${table}_raw")
    val rowContent = new java.util.HashMap[String, AnyRef](rec.data)
    rowContent.put("_event_op", rec.op)
    rowContent.put("_ingest_ts", java.time.Instant.now().toString)
    rowContent.put("_table_name", rec.tableName)
    rowContent.put("_zone", "raw")
    val insertAllRequest = InsertAllRequest.newBuilder(tableId).addRow(rowContent.asInstanceOf[java.util.Map[String, AnyRef]]).build()
    val response = bq.insertAll(insertAllRequest)
    if (!response.hasErrors) ctx.output(rec.copy(zone = "raw"))
  }
}