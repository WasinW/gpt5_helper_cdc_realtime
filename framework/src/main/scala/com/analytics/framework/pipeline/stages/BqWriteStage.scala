package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO => BQ}
import com.google.api.services.bigquery.model.TableRow
import scala.jdk.CollectionConverters._

class BqWriteStage(dataset:String, table:String) extends BaseStage[Map[String,Any], Map[String,Any]] {
  val name = s"BqWrite($dataset.$table)"
  def apply(p: Pipeline, in: PCollection[Map[String,Any]])(implicit ctx: PipelineCtx): PCollection[Map[String,Any]] = {
    val rows = in.apply("MapToRow", ParDo.of(new DoFn[Map[String,Any], TableRow](){
      @ProcessElement def proc(c: DoFn[Map[String,Any], TableRow]#ProcessContext): Unit = {
        val tr = new TableRow()
        c.element().foreach{ case(k,v) => tr.set(k, if (v==null) null else v.toString) }
        c.output(tr)
      }
    }))
    rows.apply(name, BQ.writeTableRows()
      .to(s"${ctx.projectId}:${dataset}.${table}")
      .withWriteDisposition(BQ.Write.WriteDisposition.WRITE_APPEND)
      .withCreateDisposition(BQ.Write.CreateDisposition.CREATE_IF_NEEDED))
    in
  }
}
