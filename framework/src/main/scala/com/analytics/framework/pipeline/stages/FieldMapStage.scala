package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
class FieldMapStage(mapping: Map[String,String]) extends BaseStage[Map[String,Any], Map[String,Any]] {
  val name = "FieldMap"
  def apply(p: Pipeline, in: PCollection[Map[String,Any]])(implicit ctx: PipelineCtx) =
    in.apply(name, ParDo.of(new DoFn[Map[String,Any], Map[String,Any]](){
      @ProcessElement def proc(c: DoFn[Map[String,Any],Map[String,Any]]#ProcessContext): Unit = {
        val row = c.element()
        val out = mapping.map{ case (dst, src) => dst -> row.getOrElse(src, null) }
        c.output(out)
      }
    }))
}
