package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import com.analytics.framework.utils.{JsonDotPath, RawIngestConfig}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
class MessageToRawStage(rawCfg: RawIngestConfig)
  extends BaseStage[NotificationStage.Notification, Map[String,Any]] {
  override def name: String = "message-to-raw"
  private def eval(n: NotificationStage.Notification, spec:String): Any =
    if (spec.startsWith("attr:")) n.attrs.getOrElse(spec.stripPrefix("attr:"), null)
    else if (spec.startsWith("json:")) JsonDotPath.eval(n.raw, spec.stripPrefix("json:"))
    else null
  def apply(p: Pipeline, in: PCollection[NotificationStage.Notification])(implicit ctx: PipelineCtx): PCollection[Map[String,Any]] = {
    in.apply(name, ParDo.of(new DoFn[NotificationStage.Notification, Map[String,Any]](){
      @ProcessElement def proc(c: DoFn[NotificationStage.Notification, Map[String,Any]]#ProcessContext): Unit = {
        val n = c.element()
        val m: Map[String,Any] = Map(
          "table" -> n.table,
          "event_type" -> n.eventType,
          "payload" -> n.raw
        )
        c.output(m)
      }
    }))
  }
}
