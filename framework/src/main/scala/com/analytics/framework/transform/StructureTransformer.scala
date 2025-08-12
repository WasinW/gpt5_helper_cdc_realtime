package com.analytics.framework.transform
import com.analytics.framework.pipeline.core.CDCRecord
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

class StructureTransformer extends DoFn[CDCRecord, CDCRecord] {
  @ProcessElement
  def process(ctx: DoFn[CDCRecord, CDCRecord]#ProcessContext): Unit = {
    val rec = ctx.element()
    ctx.output(rec.copy(zone = "structure"))
  }
}
