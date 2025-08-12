package com.analytics.framework.pipeline.stages

import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO => BQ}
import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

/**
 * Stage that reads rows from a BigQuery table and converts them into a
 * collection of Scala maps.  Each element in the resulting PCollection is a
 * Map[String, Any] keyed by column name.  The input PCollection is ignored
 * because this is a source stage.
 *
 * @param dataset BigQuery dataset name (without project prefix)
 * @param table   BigQuery table name
 */
class BqReadStage(dataset: String, table: String)
    extends BaseStage[Any, Map[String, Any]] {
  override def name: String = s"BqRead($dataset.$table)"

  /**
   * Read from BigQuery and map TableRow objects into Scala maps.
   *
   * @param p   the Beam pipeline
   * @param in  unused input collection
   * @param ctx implicit pipeline context providing the project ID
   * @return a PCollection of maps representing BigQuery rows
   */
  override def apply(p: Pipeline, in: PCollection[Any])(implicit ctx: PipelineCtx): PCollection[Map[String, Any]] = {
    val rows = p.apply(name, BQ.readTableRows().from(s"${ctx.projectId}:$dataset.$table"))
    rows.apply("RowToMap", ParDo.of(new DoFn[TableRow, Map[String, Any]]() {
      @ProcessElement
      def proc(c: DoFn[TableRow, Map[String, Any]]#ProcessContext): Unit = {
        val tr = c.element()
        import scala.collection.JavaConverters._
        val m = tr.keySet.asScala.map(k => k -> tr.get(k)).toMap
        c.output(m)
      }
    }))
  }
}
