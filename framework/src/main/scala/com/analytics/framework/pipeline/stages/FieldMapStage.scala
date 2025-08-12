package com.analytics.framework.pipeline.stages

import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

/**
 * Stage that remaps fields of an input map according to a provided mapping.
 * Each entry in the mapping defines a destination field name and the source
 * field name to copy from the input map.  Missing values are replaced with
 * null.
 *
 * @param mapping map of destination field names to source field names
 */
class FieldMapStage(mapping: Map[String, String])
    extends BaseStage[Map[String, Any], Map[String, Any]] {
  override def name: String = "FieldMap"

  /**
   * Apply the field mapping to each record in the input PCollection.
   *
   * @param p   the Beam pipeline
   * @param in  input collection of maps
   * @param ctx implicit pipeline context (unused)
   * @return a PCollection of remapped maps
   */
  override def apply(p: Pipeline, in: PCollection[Map[String, Any]])(implicit ctx: PipelineCtx): PCollection[Map[String, Any]] = {
    in.apply(name, ParDo.of(new DoFn[Map[String, Any], Map[String, Any]]() {
      @ProcessElement
      def proc(c: DoFn[Map[String, Any], Map[String, Any]]#ProcessContext): Unit = {
        val row = c.element()
        val out = mapping.map { case (dst, src) => dst -> row.getOrElse(src, null) }
        c.output(out)
      }
    }))
  }
}
