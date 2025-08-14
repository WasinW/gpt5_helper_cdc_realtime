package com.analytics.framework.pipeline.stages

import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import scala.collection.JavaConverters._

/**
 * Stage that converts Pub/Sub messages into `Notification` records.  A
 * `Notification` contains common attributes such as accountId, eventType,
 * table and the raw JSON payload.
 */
object NotificationStage {
  /**
   * Parsed representation of a Pub/Sub message used by downstream stages.
   *
   * @param accountId the account identifier associated with the notification
   * @param eventType the type of event (e.g. INSERT, UPDATE, DELETE)
   * @param table     the table affected by the event
   * @param raw       the raw JSON payload
   * @param attrs     all message attributes as a Scala map
   */
  case class Notification(
      accountId: String,
      eventType: String,
      table: String,
      raw: String,
      attrs: Map[String, String]
  )
}

/**
 * Stage that decodes incoming Pub/Sub messages into Notification objects.
 */
class NotificationStage() extends BaseStage[PubsubMessage, NotificationStage.Notification] {
  override def name: String = "notification"

  /**
   * Extract attributes and payload from each Pub/Sub message.
   *
   * @param p   the Beam pipeline
   * @param in  input collection of PubsubMessage
   * @param ctx implicit pipeline context (unused)
   * @return a PCollection of `Notification` objects
   */
  override def apply(p: Pipeline, in: PCollection[PubsubMessage])(implicit ctx: PipelineCtx): PCollection[NotificationStage.Notification] = {
    in.apply(name, ParDo.of(new DoFn[PubsubMessage, NotificationStage.Notification]() {
      @ProcessElement
      def proc(c: DoFn[PubsubMessage, NotificationStage.Notification]#ProcessContext): Unit = {
        val msg = c.element()
        val attrs = msg.getAttributeMap.asScala.map { case (k, v) => (k, String.valueOf(v)) }.toMap
        val table = attrs.getOrElse("table_name", "")
        val et = attrs.getOrElse("event_type", "")
        val acct = attrs.getOrElse("account_id", "")
        val raw = new String(msg.getPayload, java.nio.charset.StandardCharsets.UTF_8)
        c.output(NotificationStage.Notification(acct, et, table, raw, attrs))
      }
    }))
  }
}
