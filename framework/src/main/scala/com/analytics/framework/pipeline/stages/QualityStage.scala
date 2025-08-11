package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx, QualityRule}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
class QualityStage[T](rules: List[QualityRule[T]], log: String => Unit) extends BaseStage[T,T] {
  val name = "QualityStage"
  def apply(p: Pipeline, in: PCollection[T])(implicit ctx: PipelineCtx): PCollection[T] = {
    in.apply(name, ParDo.of(new DoFn[T,T](){
      @ProcessElement def proc(c: DoFn[T,T]#ProcessContext): Unit = {
        val row = c.element()
        val failed = rules.filterNot(_.check(row, ctx))
        if (failed.nonEmpty) log(s"""{"type":"dq","rules":[${failed.map(_.id).mkString(",")}]}""")
        c.output(row)
      }
    }))
  }
}
