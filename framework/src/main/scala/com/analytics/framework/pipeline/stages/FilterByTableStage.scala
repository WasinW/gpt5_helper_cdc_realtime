package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
class FilterByTableStage(table:String) extends BaseStage[Map[String,Any], Map[String,Any]] {
  val name = s"FilterByTable($table)"
  def apply(p: Pipeline, in: PCollection[Map[String,Any]])(implicit ctx: PipelineCtx): PCollection[Map[String,Any]] =
    in.apply(name, ParDo.of(new DoFn[Map[String,Any], Map[String,Any]](){
      @ProcessElement def proc(c: DoFn[Map[String,Any], Map[String,Any]]#ProcessContext): Unit = {
        val e = c.element(); if (e.getOrElse("table","") == table) c.output(e)
      }
    }))
}
