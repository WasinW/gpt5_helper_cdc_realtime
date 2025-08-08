package com.analytics.framework.pipeline.core

import org.apache.beam.sdk.transforms.DoFn
import com.analytics.framework.connectors.FetchedData

case class CDCRecord(recordId: String, op: String, data: java.util.Map[String, AnyRef], tableName: String, zone: String)

class CDCDetector(opField: String = "op_type", deleteFlag: String = "is_deleted", tableNameField: String = "_table_name", zoneField: String = "_zone")
  extends DoFn[FetchedData, CDCRecord] {
  @org.apache.beam.sdk.transforms.DoFn.ProcessElement
  def process(ctx: DoFn[FetchedData, CDCRecord]#ProcessContext): Unit = {
    val f = ctx.element()
    val opRaw = Option(f.row.get(opField)).map(_.toString)
    val del = Option(f.row.get(deleteFlag)).exists(v => v != null && v.toString.equalsIgnoreCase("true"))
    val op = if (del) "delete" else opRaw.getOrElse("upsert")
    val tableName = Option(f.row.get(tableNameField)).map(_.toString).getOrElse("unknown")
    ctx.output(CDCRecord(f.recordId, op, f.row, tableName, "raw"))
  }
}