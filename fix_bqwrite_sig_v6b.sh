#!/usr/bin/env bash
set -euo pipefail

cat > framework/src/main/scala/com/analytics/framework/pipeline/stages/BqWriteDynamicStage.scala <<'SCALA'
package com.analytics.framework.pipeline.stages

import com.google.api.services.bigquery.model.{TableRow, TableSchema}
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, DynamicDestinations, TableDestination}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.{PCollection, ValueInSingleWindow}
import org.apache.beam.sdk.Pipeline

import com.analytics.framework.core.base.{BaseStage, PipelineCtx}

class BqWriteDynamicStage(dataset: String)
  extends BaseStage[Map[String,Any], Map[String,Any]] {

  override def name: String = s"BqWriteDynamic($dataset)"

  def apply(p: Pipeline, in: PCollection[Map[String,Any]])
           (implicit ctx: PipelineCtx): PCollection[Map[String,Any]] = {

    val project = ctx.projectId
    val ds      = dataset

    // Map Scala Map -> TableRow (ต้องมี key "table" สำหรับเลือกปลายทาง)
    val tableRows = in.apply("MapToTableRow", ParDo.of(new DoFn[Map[String,Any], TableRow] {
      @ProcessElement
      def proc(c: DoFn[Map[String,Any], TableRow]#ProcessContext): Unit = {
        val m  = c.element()
        val tr = new TableRow()
        m.foreach{ case (k,v) => tr.set(k, if (v==null) null else v.toString) }
        if (!m.contains("table")) tr.set("table", "unknown")
        c.output(tr)
      }
    }))

    // เขียน BigQuery แบบ DynamicDestinations
    tableRows.apply("bqDynamicWrite",
      BigQueryIO.writeTableRows()
        .to(new DynamicDestinations[TableRow, String](){
          override def getDestination(e: ValueInSingleWindow[TableRow]): String = {
            val tr    = e.getValue
            val table = Option(tr.get("table")).map(_.toString).getOrElse("unknown")
            s"$project:$ds.$table"
          }
          override def getTable(dest: String): TableDestination =
            new TableDestination(dest, s"dynamic to $dest")
          override def getSchema(dest: String): TableSchema = null // CREATE_NEVER → ไม่ต้อง schema
        })
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
    )

    // passthrough collection เดิม (stage นี้ทำ side-effect write BQ)
    in
  }
}
SCALA

echo "patched BqWriteDynamicStage.scala"
