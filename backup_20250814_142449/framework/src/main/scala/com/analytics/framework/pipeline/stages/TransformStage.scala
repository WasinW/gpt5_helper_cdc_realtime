package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.PipelineCtx
import com.analytics.framework.modules.transform.TransformModule

class TransformStage[I, O](module: TransformModule[I, O]) extends BaseStage[I, O] {
  override def name: String = s"TransformStage(${module.getClass.getSimpleName})"
  override def run(ctx: PipelineCtx, in: Seq[I]): Seq[O] = in.map(x => module.transform(ctx, x))
}
