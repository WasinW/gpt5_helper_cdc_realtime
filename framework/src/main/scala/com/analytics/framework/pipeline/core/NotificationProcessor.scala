package com.analytics.framework.pipeline.core

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import com.google.gson.{Gson, JsonParser, JsonElement}
import scala.jdk.CollectionConverters._

class NotificationProcessor(config: java.util.Map[String, Object], table: String) extends DoFn[PubsubMessage, NotificationEvent] {
  @transient private var gson: Gson = _
  @transient private var parser: JsonParser = _

  @org.apache.beam.sdk.transforms.DoFn.Setup
  def setup(): Unit = {
    gson = new Gson()
    parser = new JsonParser()
  }

  @org.apache.beam.sdk.transforms.DoFn.ProcessElement
  def process(ctx: DoFn[PubsubMessage, NotificationEvent]#ProcessContext): Unit = {
    val msg = ctx.element()
    val payload = new String(msg.getPayload, java.nio.charset.StandardCharsets.UTF_8)
    val attr = msg.getAttributeMap.asScala

    if (attr.get("table_name").exists(_ == table)) {
      val json: JsonElement = parser.parse(payload)
      val recordIds: java.util.List[String] =
        if (json.isJsonArray) json.getAsJsonArray.iterator().asScala.map(_.getAsString).toList.asJava
        else if (json.isJsonObject && json.getAsJsonObject.has("record_ids"))
          json.getAsJsonObject.getAsJsonArray("record_ids").iterator().asScala.map(_.getAsString).toList.asJava
        else java.util.Arrays.asList(json.getAsJsonObject.get("record_id").getAsString)

      val out = new NotificationEvent(
        attr.getOrElse("message_id", java.util.UUID.randomUUID().toString),
        attr.getOrElse("event_type", "update"),
        table,
        recordIds,
        attr.getOrElse("event_timestamp", java.time.Instant.now().toString)
      )
      ctx.output(out)
    }
  }
}

case class NotificationEvent(
  messageId: String,
  eventType: String,
  tableName: String,
  recordIds: java.util.List[String],
  timestamp: String
)