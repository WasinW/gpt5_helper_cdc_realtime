#!/usr/bin/env bash
set -euo pipefail

cat > framework/src/main/scala/com/analytics/framework/pipeline/stages/BqWriteDynamicStage.scala <<'SCALA'
package com.analytics.framework.pipeline.stages

import com.google.api.services.bigquery.model.{TableRow, TableSchema, TableDestination}
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO, DynamicDestinations}
import org.apache.beam.sdk.transforms.{DoFn, MapElements}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.values.{PCollection, TypeDescriptors, ValueInSingleWindow}
import org.apache.beam.sdk.Pipeline

import com.analytics.framework.core.base.{BaseStage, PipelineCtx}

class BqWriteDynamicStage(dataset: String)
  extends BaseStage[Map[String,Any], Map[String,Any]] {

  override def name: String = s"BqWriteDynamic($dataset)"

  def apply(p: Pipeline, in: PCollection[Map[String,Any]])
           (implicit ctx: PipelineCtx): PCollection[Map[String,Any]] = {

    val project = ctx.projectId
    val ds      = dataset

    // Map Scala Map -> TableRow (เก็บชื่อ table ที่ field "table")
    val rows = in.apply("toTableRow", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers())) // just to force type; we'll use a DoFn instead
      .via((m: Map[String,Any]) => m)) // placeholder, we switch to DoFn below to build TableRow
    // แทน MapElements บน Map → TableRow ด้วย DoFn เพื่อความชัดเจน
    val toRow = org.apache.beam.sdk.transforms.ParDo.of(new DoFn[Map[String,Any], TableRow] {
      @ProcessElement
      def proc(c: DoFn[Map[String,Any], TableRow]#ProcessContext): Unit = {
        val m = c.element()
        val tr = new TableRow()
        m.foreach{ case (k,v) => tr.set(k, if (v==null) null else v.toString) }
        // ต้องมีคอลัมน์ "table" ใช้กำหนดปลายทาง (ตามที่เราวางสเปคไว้)
        // ถ้าไม่มี ให้โยนเป็น default "unknown"
        if (!m.contains("table")) tr.set("table", "unknown")
        c.output(tr)
      }
    })
    val tableRows = in.apply("MapToTableRow", toRow)

    // เขียนแบบ DynamicDestinations
    tableRows.apply("bqDynamicWrite",
      BigQueryIO.writeTableRows()
        .to(new DynamicDestinations[TableRow, String](){
          override def getDestination(e: ValueInSingleWindow[TableRow]): String = {
            val tr = e.getValue
            val table = Option(tr.get("table")).map(_.toString).getOrElse("unknown")
            s"$project:$ds.$table"
          }
          override def getTable(dest: String): TableDestination =
            new TableDestination(dest, s"dynamic to $dest")

          // สำหรับ CREATE_NEVER ไม่จำเป็นต้องให้ schema ก็ได้ (ตารางต้องมีอยู่แล้ว)
          override def getSchema(dest: String): TableSchema = null
        })
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
    )

    // passthrough collection เดิม (stage นี้ side-effect: write BQ)
    in
  }
}
SCALA

echo "patched BqWriteDynamicStage.scala"
