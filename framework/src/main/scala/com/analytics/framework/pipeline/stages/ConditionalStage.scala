package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
class ConditionalStage[T](pred: T => Boolean) extends BaseStage[T,T] {
  val name = "ConditionalStage"
  def apply(p: Pipeline, in: PCollection[T])(implicit ctx: PipelineCtx): PCollection[T] = {
    in.apply(name, ParDo.of(new DoFn[T,T](){
      @ProcessElement def proc(c: DoFn[T,T]#ProcessContext): Unit = if (pred(c.element())) c.output(c.element())
    }))
  }
}
