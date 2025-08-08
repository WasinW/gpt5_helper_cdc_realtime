package com.analytics.framework.pipeline.core

import org.apache.beam.sdk.transforms.DoFn

class AuditLogger(project: String, domain: String, table: String, zone: String)
  extends DoFn[CDCRecord, Void] {
  @org.apache.beam.sdk.transforms.DoFn.ProcessElement
  def process(ctx: DoFn[CDCRecord, Void]#ProcessContext): Unit = {
    val rec = ctx.element()
    // TODO: write JSON log to GCS or BigQuery framework tables
    System.out.println(s"AUDIT ${project}/${domain}/${table}/${zone} record=${rec.recordId}")
  }
}