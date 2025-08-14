package com.analytics.framework.transform

import org.apache.beam.sdk.transforms.DoFn
import com.analytics.framework.pipeline.core.CDCRecord

class RefinedTransformation extends DoFn[CDCRecord, CDCRecord] {
  @org.apache.beam.sdk.transforms.DoFn.ProcessElement
  def process(ctx: DoFn[CDCRecord, CDCRecord]#ProcessContext): Unit = {
    ctx.output(ctx.element().copy(zone = "refined"))
  }
}