package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.PipelineCtx

trait BaseStage[I, O] {
  def name: String
  def run(ctx: PipelineCtx, in: Seq[I]): Seq[O]
}
