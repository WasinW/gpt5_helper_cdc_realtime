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
    // Simple insert via streaming API (insertAll). For prod, consider Storage Write API.
    val tableId = TableId.of(dataset, s"${table}_raw")
    val rowContent = rec.data.asScala.toMap + ("_event_op" -> rec.op, "_ingest_ts" -> java.time.Instant.now().toString)
    val insertAllRequest = InsertAllRequest.newBuilder(tableId).addRow(rowContent.asJava).build()
    val response = bq.insertAll(insertAllRequest)
    if (!response.hasErrors) ctx.output(rec) else {
      // Optionally: write to DLQ
    }
  }
}