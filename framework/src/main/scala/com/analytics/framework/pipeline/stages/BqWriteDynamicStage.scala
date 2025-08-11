package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.{PCollection, ValueInSingleWindow}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO => BQ}
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations
import com.google.api.services.bigquery.model.{TableRow, TableDestination}
import scala.jdk.CollectionConverters._

class BqWriteDynamicStage(dataset:String, tableFn: Map[String,Any] => String)
  extends BaseStage[Map[String,Any], Map[String,Any]] {

  val name = s"BqWriteDynamic($dataset)"

  def apply(p: Pipeline, in: PCollection[Map[String,Any]])(implicit ctx: PipelineCtx): PCollection[Map[String,Any]] = {
    val rows = in.apply("MapToRow", ParDo.of(new DoFn[Map[String,Any], TableRow](){
      @ProcessElement def proc(c: DoFn[Map[String,Any], TableRow]#ProcessContext): Unit = {
        val tr = new TableRow()
        c.element().foreach{ case(k,v) => tr.set(k, if (v==null) null else v.toString) }
        // เก็บ tableName ลง attribute "__dest"
        tr.set("__dest", tableFn(c.element()))
        c.output(tr)
      }
    }))

    rows.apply(name,
      BQ.writeTableRows()
        .to(new DynamicDestinations[TableRow, String](){
          override def getDestination(value: TableRow): String =
            s"${ctx.projectId}:${dataset}.${value.get("__dest").asInstanceOf[String]}"
          override def getTable(value: String): TableDestination =
            new TableDestination(value, s"dynamic to $value")
          override def getSchema(value: String) = null // ใช้ auto schema (ให้ BQ สร้าง/append)
        })
        .withCreateDisposition(BQ.Write.CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(BQ.Write.WriteDisposition.WRITE_APPEND)
    )
    in
  }
}
