package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import java.nio.charset.StandardCharsets

class NotificationStage() extends BaseStage[PubsubMessage, NotificationStage.Notification] {
  val name = "NotificationStage"
  def apply(p: Pipeline, in: PCollection[PubsubMessage])(implicit ctx: PipelineCtx): PCollection[NotificationStage.Notification] = {
    in.apply(name, ParDo.of(new DoFn[PubsubMessage, NotificationStage.Notification](){
      @ProcessElement def proc(c: DoFn[PubsubMessage, NotificationStage.Notification]#ProcessContext): Unit = {
        val msg = c.element()
        val data = new String(msg.getPayload(), StandardCharsets.UTF_8)
        val attrs = msg.getAttributeMap()
        val accountId = Option(attrs.get("accountId")).getOrElse("")
        val eventType = Option(attrs.get("event_type")).orElse(Option(attrs.get("eventType"))).getOrElse("")
        val tableName = Option(attrs.get("table_name")).getOrElse(Option(attrs.get("table")).getOrElse(""))
        c.output(NotificationStage.Notification(accountId=accountId, eventType=eventType, table=tableName, raw=data))
      }
    }))
  }
}

object NotificationStage {
  case class Notification(accountId:String, eventType:String, table:String, raw:String)
  def readFrom(p: Pipeline, subscription: String): PCollection[PubsubMessage] =
    p.apply(s"PubSubRead:$subscription", PubsubIO.readMessagesWithAttributes().fromSubscription(subscription))
}
