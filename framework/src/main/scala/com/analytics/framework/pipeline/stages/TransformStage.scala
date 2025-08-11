package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx, TransformModule}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
class TransformStage[I,O](moduleClass:String, moduleConfig: Map[String,Any]) extends BaseStage[I,O] {
  val name = s"Transform($moduleClass)"
  @transient private var mod: TransformModule[I,O] = _
  private def ensure(implicit ctx: PipelineCtx): Unit = {
    if (mod == null) {
      mod = Class.forName(moduleClass).getDeclaredConstructor().newInstance().asInstanceOf[TransformModule[I,O]]
      mod.init(moduleConfig, ctx)
    }
  }
  def apply(p: Pipeline, in: PCollection[I])(implicit ctx: PipelineCtx) = {
    ensure(ctx)
    in.apply(name, ParDo.of(new DoFn[I,O](){ @ProcessElement def proc(c: DoFn[I,O]#ProcessContext): Unit =
      mod.exec(c.element(), ctx).foreach(c.output) }))
  }
}
