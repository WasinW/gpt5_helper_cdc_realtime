package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO => BQ, DynamicDestinations}
import com.google.api.services.bigquery.model.{TableRow, TableSchema, TableDestination}
import scala.jdk.CollectionConverters._

class BqWriteDynamicStage(dataset:String, tableFn: Map[String,Any] => String)
  extends BaseStage[Map[String,Any], Map[String,Any]] {
  val name = s"BqWriteDynamic($dataset)"
  def apply(p: Pipeline, in: PCollection[Map[String,Any]])(implicit ctx: PipelineCtx): PCollection[Map[String,Any]] = {
    val rows = in.apply("MapToRow", ParDo.of(new DoFn[Map[String,Any], TableRow](){
      @ProcessElement def proc(c: DoFn[Map[String,Any], TableRow]#ProcessContext): Unit = {
        val tr = new TableRow(); val e = c.element()
        e.foreach{ case(k,v) => tr.set(k, if (v==null) null else v.toString) }
        tr.set("__dest", tableFn(e))
        c.output(tr)
      }
    }))

    rows.apply(name,
      BQ.writeTableRows()
        .to(new DynamicDestinations[TableRow, String](){
          override def getDestination(value: TableRow): String =
            s"${ctx.projectId}:${dataset}.${value.get("__dest").asInstanceOf[String]}"
          override def getTable(dest: String): TableDestination =
            new TableDestination(dest, s"dynamic to $dest")
          override def getSchema(dest: String): TableSchema = null
        })
        .withCreateDisposition(BQ.Write.CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(BQ.Write.WriteDisposition.WRITE_APPEND)
    )
    in
  }
}
