package com.analytics.framework.pipeline.core
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

class RawInserter extends DoFn[CDCRecord, CDCRecord] {
  @ProcessElement
  def process(ctx: DoFn[CDCRecord, CDCRecord]#ProcessContext): Unit = {
    val rec = ctx.element()
    // stub: ถือว่า insert สำเร็จ แล้ว forward พร้อม zone = raw
    ctx.output(rec.copy(zone = "raw"))
  }
}
