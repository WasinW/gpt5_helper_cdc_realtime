package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
class BqWriteStage(dataset:String, table:String) extends BaseStage[Map[String,Any], Map[String,Any]]{
  override def name: String = s"bq-write:$dataset.$table"
  def apply(p:Pipeline, in:PCollection[Map[String,Any]])(implicit ctx:PipelineCtx): PCollection[Map[String,Any]] = in
}
