package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.io.gcp.pubsub.{PubsubIO, PubsubMessage}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

class NotificationStage() extends BaseStage[PubsubMessage, NotificationStage.Notification] {
  val name = "NotificationStage"
  def apply(p: Pipeline, in: PCollection[PubsubMessage])(implicit ctx: PipelineCtx) =
    in.apply(name, ParDo.of(new DoFn[PubsubMessage, NotificationStage.Notification](){
      @ProcessElement def proc(c: DoFn[PubsubMessage, NotificationStage.Notification]#ProcessContext): Unit = {
        val msg   = c.element()
        val data  = new String(msg.getPayload, StandardCharsets.UTF_8)
        val attrs = msg.getAttributeMap.asScala.view.mapValues(String.valueOf(_)).toMap
        val accountId = attrs.getOrElse("accountId","")
        val eventType = attrs.getOrElse("event_type", attrs.getOrElse("eventType",""))
        val tableName = attrs.getOrElse("table_name", attrs.getOrElse("table",""))
        c.output(NotificationStage.Notification(accountId, eventType, tableName, data, attrs))
      }
    }))
}
object NotificationStage {
  case class Notification(accountId:String, eventType:String, table:String, raw:String, attrs: Map[String,String])
  def readFrom(p: Pipeline, subscription: String): PCollection[PubsubMessage] =
    p.apply(s"PubSubRead:$subscription", PubsubIO.readMessagesWithAttributes().fromSubscription(subscription))
}
