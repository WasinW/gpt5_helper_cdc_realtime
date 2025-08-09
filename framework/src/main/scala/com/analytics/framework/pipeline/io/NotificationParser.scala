package com.analytics.framework.pipeline.io
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.apache.beam.sdk.transforms.DoFn
import scala.jdk.CollectionConverters._
import com.analytics.framework.pipeline.model.Notification
class NotificationParser extends DoFn[PubsubMessage, Notification] {
  @DoFn.ProcessElement
  def process(ctx: DoFn[PubsubMessage, Notification]#ProcessContext): Unit = {
    val m = ctx.element()
    val attr = m.getAttributeMap.asScala
    val accountId = attr.getOrElse("accountId", "")
    val eventType = attr.getOrElse("event_type", if (attr.getOrElse("topic","") contains "create") "create" else "update")
    val ts = attr.getOrElse("event_timestamp", java.time.Instant.now().toString)
    ctx.output(Notification(attr.getOrElse("message_id", java.util.UUID.randomUUID().toString), eventType, accountId, ts))
  }
}
