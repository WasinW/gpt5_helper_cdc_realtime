package com.analytics.framework.modules.transform
import com.analytics.framework.core.base.PipelineCtx

trait TransformModule[I, O] {
  def transform(ctx: PipelineCtx, in: I): O
}
