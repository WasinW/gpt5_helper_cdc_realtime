package com.analytics.framework.pipeline.stages

import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

/**
 * Stage that filters records based on the value of their "table" field.
 * Only records whose "table" field matches the configured table name are
 * passed through.
 *
 * @param table the table name to filter on
 */
class FilterByTableStage(table: String)
    extends BaseStage[Map[String, Any], Map[String, Any]] {
  override def name: String = s"FilterByTable($table)"

  /**
   * Filter the input collection to records whose table field matches.
   *
   * @param p   the Beam pipeline
   * @param in  input collection of maps
   * @param ctx implicit pipeline context (unused)
   * @return a PCollection containing only maps with the desired table name
   */
  override def apply(p: Pipeline, in: PCollection[Map[String, Any]])(implicit ctx: PipelineCtx): PCollection[Map[String, Any]] = {
    in.apply(name, ParDo.of(new DoFn[Map[String, Any], Map[String, Any]]() {
      @ProcessElement
      def proc(c: DoFn[Map[String, Any], Map[String, Any]]#ProcessContext): Unit = {
        val e = c.element()
        if (e.getOrElse("table", "") == table) c.output(e)
      }
    }))
  }
}
