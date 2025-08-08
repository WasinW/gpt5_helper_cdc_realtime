package com.analytics.framework.pipeline.core

import org.apache.beam.sdk.transforms.DoFn
import com.analytics.framework.connectors.FetchedData

case class CDCRecord(recordId: String, op: String, data: java.util.Map[String, AnyRef])

class CDCDetector extends DoFn[FetchedData, CDCRecord] {
  @org.apache.beam.sdk.transforms.DoFn.ProcessElement
  def process(ctx: DoFn[FetchedData, CDCRecord]#ProcessContext): Unit = {
    val f = ctx.element()
    // Naive CDC op detection using a 'op_type' column if exists else default update
    val op = Option(f.row.get("op_type")).map(_.toString).getOrElse("upsert")
    ctx.output(CDCRecord(f.recordId, op, f.row))
  }
}