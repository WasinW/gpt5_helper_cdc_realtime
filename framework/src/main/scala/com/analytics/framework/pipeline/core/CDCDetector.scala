package com.analytics.framework.pipeline.core
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

final case class CDCRecord(data: Map[String,Any], zone: String = "")

class CDCDetector extends DoFn[Map[String,Any], CDCRecord] {
  @ProcessElement
  def process(ctx: DoFn[Map[String,Any], CDCRecord]#ProcessContext): Unit = {
    // stub: pass-through wrap to CDCRecord
    val m = ctx.element()
    ctx.output(CDCRecord(m))
  }
}
