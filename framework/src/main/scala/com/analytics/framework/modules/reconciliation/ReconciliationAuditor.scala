package com.analytics.framework.modules.reconciliation

import org.apache.beam.sdk.transforms.DoFn
import com.google.cloud.bigquery._
import scala.jdk.CollectionConverters._

class ReconciliationAuditor(projectId: String, frameworkDataset: String)
  extends DoFn[ReconcileResult, Void] {

  @transient private var bq: BigQuery = _

  @org.apache.beam.sdk.transforms.DoFn.Setup
  def setup(): Unit = { bq = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService }

  @org.apache.beam.sdk.transforms.DoFn.ProcessElement
  def process(ctx: DoFn[ReconcileResult, Void]#ProcessContext): Unit = {
    val r = ctx.element()
    val tableId = TableId.of(frameworkDataset, "reconcile_log")
    val row = Map(
      "reconcile_id" -> java.util.UUID.randomUUID().toString,
      "domain" -> frameworkDataset.replaceAll("_framework$", ""),
      "zone" -> r.zone,
      "table_name" -> r.tableName,
      "window_start" -> r.windowStart,
      "window_end" -> r.windowEnd,
      "gcp_count" -> Long.box(r.gcpCount),
      "aws_count" -> Long.box(r.awsCount),
      "count_match" -> Boolean.box(r.countMatch),
      "mismatch_samples" -> r.mismatchSamples.map(_.toString).asJava
    ).asJava.asInstanceOf[java.util.Map[String, AnyRef]]
    bq.insertAll(InsertAllRequest.newBuilder(tableId).addRow(row).build())
  }
}