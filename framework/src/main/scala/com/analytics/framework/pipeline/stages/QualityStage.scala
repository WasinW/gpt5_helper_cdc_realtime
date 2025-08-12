package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
class QualityStage[T <: Map[String,Any]](notNullCols: List[String], audit: String => Unit)
  extends BaseStage[T,T]{
  override def name: String = "quality_stage"
  def apply(p:Pipeline, in:PCollection[T])(implicit ctx:PipelineCtx): PCollection[T] = {
    in.apply("dq-check", ParDo.of(new DoFn[T,T](){
      @ProcessElement def proc(c: DoFn[T,T]#ProcessContext): Unit = {
        val m = c.element()
        val fails = notNullCols.filter(col => !m.contains(col) || m.get(col)==null)
        if (fails.nonEmpty) audit(s"""{"level":"WARN","type":"not_null","cols":"${fails.mkString(",")}","row":"$m"}""")
        c.output(m)
      }
    }))
  }
}
