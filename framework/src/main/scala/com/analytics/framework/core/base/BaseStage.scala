package com.analytics.framework.core.base
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
trait BaseStage[I,O] extends Serializable {
  def name: String
  def apply(p: Pipeline, in: PCollection[I])(implicit ctx: PipelineCtx): PCollection[O]
}
