package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import com.analytics.framework.utils.JsonDotPath
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.PCollection

class MessageToRawStage(rawCfg: RawIngestConfig)
  extends BaseStage[NotificationStage.Notification, Map[String,Any]] {
  val name = "MessageToRaw"
  private def evalSpec(n: NotificationStage.Notification, spec: String): Any = {
    if (spec.startsWith("attr:")) n.attrs.getOrElse(spec.stripPrefix("attr:"), null)
    else if (spec.startsWith("json:")) JsonDotPath.eval(n.raw, spec.stripPrefix("json:"))
    else if (spec == "now_millis") java.lang.Long.valueOf(System.currentTimeMillis())
    else null
  }
  def apply(p: Pipeline, in: PCollection[NotificationStage.Notification])(implicit ctx: PipelineCtx): PCollection[Map[String,Any]] = {
    in.apply(name, ParDo.of(new DoFn[NotificationStage.Notification, Map[String,Any]](){
      @ProcessElement def proc(c: DoFn[NotificationStage.Notification, Map[String,Any]]#ProcessContext): Unit = {
        val n = c.element()
        val table = n.attrs.getOrElse(rawCfg.routingAttr, n.table)
        if (rawCfg.allowedTables.contains(table)) {
          val tCols = rawCfg.columnsByTable.getOrElse(table, Map.empty)
          val merged = rawCfg.defaultCols ++ tCols
          val mapped = merged.map{ case (dst, spec) => dst -> evalSpec(n, spec) } + ("table" -> table)
          c.output(mapped)
        }
      }
    }))
  }
}
