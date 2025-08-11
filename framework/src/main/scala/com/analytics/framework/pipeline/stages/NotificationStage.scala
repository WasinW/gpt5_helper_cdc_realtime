package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import java.nio.charset.StandardCharsets

final case class Notification(accountId:String, eventType:String, raw:String)

class NotificationStage(subscription: String) extends BaseStage[PubsubMessage, Notification] {
  val name = "NotificationStage"
  def apply(p: Pipeline, in: PCollection[PubsubMessage])(implicit ctx: PipelineCtx): PCollection[Notification] = {
    in.apply(name, ParDo.of(new DoFn[PubsubMessage, Notification](){
      @ProcessElement def proc(c: DoFn[PubsubMessage, Notification]#ProcessContext): Unit = {
        val msg = c.element()
        val data = new String(msg.getPayload(), StandardCharsets.UTF_8)
        val attrs = msg.getAttributeMap()
        val accountId = Option(attrs.get("accountId")).getOrElse("")
        val eventType = Option(attrs.get("event_type")).orElse(Option(attrs.get("eventType"))).getOrElse("")
        c.output(Notification(accountId=accountId, eventType=eventType, raw=data))
      }
    }))
  }
}

object NotificationStage {
  def readFrom(p: Pipeline, subscription: String): PCollection[PubsubMessage] =
    p.apply("PubSubRead", PubsubIO.readMessagesWithAttributes().fromSubscription(subscription))
}
