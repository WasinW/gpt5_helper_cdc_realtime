package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryIO => BQ}
import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
class BqReadStage(dataset:String, table:String) extends BaseStage[Any, Map[String,Any]] {
  val name = s"BqRead($dataset.$table)"
  def apply(p: Pipeline, in: PCollection[Any])(implicit ctx: PipelineCtx): PCollection[Map[String,Any]] = {
    val rows = p.apply(name, BQ.readTableRows().from(s"${ctx.projectId}:${dataset}.${table}"))
    rows.apply("RowToMap", ParDo.of(new DoFn[TableRow, Map[String,Any]](){
      @ProcessElement def proc(c: DoFn[TableRow, Map[String,Any]]#ProcessContext): Unit = {
        val tr = c.element()
        import scala.jdk.CollectionConverters._
        val m = tr.keySet.asScala.map(k => k -> tr.get(k)).toMap
        c.output(m)
      }
    }))
  }
}
