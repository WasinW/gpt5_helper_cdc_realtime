package com.analytics.framework.pipeline.stages

import com.google.api.services.bigquery.model.{TableRow, TableSchema}
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, DynamicDestinations, TableDestination}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.{PCollection, ValueInSingleWindow}
import org.apache.beam.sdk.Pipeline

import com.analytics.framework.core.base.{BaseStage, PipelineCtx}

/**
 * Stage that writes a stream of maps into BigQuery using dynamic table
 * destinations.  Each input map must contain a "table" key which specifies
 * the destination table name; if the key is missing the record will be
 * written to a table named "unknown".  The write is performed as a
 * side‑effect; the original input PCollection is passed through unchanged.
 *
 * @param dataset the BigQuery dataset where tables reside
 */
class BqWriteDynamicStage(dataset: String)
    extends BaseStage[Map[String, Any], Map[String, Any]] {
  override def name: String = s"BqWriteDynamic($dataset)"

  /**
   * Writes the input maps to BigQuery tables determined at runtime.
   *
   * @param p   the Beam pipeline
   * @param in  the input collection of maps (must include a "table" key)
   * @param ctx implicit pipeline context providing the project ID
   * @return the input collection unchanged
   */
  override def apply(p: Pipeline, in: PCollection[Map[String, Any]])(implicit ctx: PipelineCtx): PCollection[Map[String, Any]] = {
    val project = ctx.projectId
    val ds = dataset
    // Map Scala Map -> TableRow (must have key "table" for choosing destination)
    val tableRows = in.apply("MapToTableRow", ParDo.of(new DoFn[Map[String, Any], TableRow] {
      @ProcessElement
      def proc(c: DoFn[Map[String, Any], TableRow]#ProcessContext): Unit = {
        val m = c.element()
        val tr = new TableRow()
        m.foreach { case (k, v) => tr.set(k, if (v == null) null else v.toString) }
        if (!m.contains("table")) tr.set("table", "unknown")
        c.output(tr)
      }
    }))
    // Write to BigQuery using DynamicDestinations
    tableRows.apply("bqDynamicWrite",
      BigQueryIO.writeTableRows()
        .to(new DynamicDestinations[TableRow, String]() {
          override def getDestination(e: ValueInSingleWindow[TableRow]): String = {
            val tr = e.getValue
            val table = Option(tr.get("table")).map(_.toString).getOrElse("unknown")
            s"$project:$ds.$table"
          }
          override def getTable(dest: String): TableDestination =
            new TableDestination(dest, s"dynamic to $dest")
          override def getSchema(dest: String): TableSchema = null // CREATE_NEVER → no schema
        })
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
    )
    // Pass through the original collection
    in
  }
}
