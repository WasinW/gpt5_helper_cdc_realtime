package com.analytics.framework.pipeline.stages

import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import com.analytics.framework.utils.{JsonDotPath, RawIngestConfig}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

/**
 * Stage that converts `Notification` objects into raw records represented as
 * maps.  Each output map contains standard fields (table, event_type,
 * payload) and may be extended using the provided `RawIngestConfig`.
 *
 * @param rawCfg configuration describing how to map fields from the notification
 */
class MessageToRawStage(rawCfg: RawIngestConfig)
    extends BaseStage[NotificationStage.Notification, Map[String, Any]] {
  override def name: String = "message-to-raw"

  /**
   * Evaluate a specification against a notification.  Specifications prefixed
   * with `attr:` read from the message attributes, while `json:` prefixes
   * read from the JSON payload using dot‑notation.  Unsupported specs return
   * null.
   */
  private def eval(n: NotificationStage.Notification, spec: String): Any = {
    if (spec.startsWith("attr:")) {
      n.attrs.getOrElse(spec.stripPrefix("attr:"), null)
    } else if (spec.startsWith("json:")) {
      // Use JsonDotPath to extract from the raw JSON payload
      JsonDotPath.eval(n.raw, spec.stripPrefix("json:")).orNull
    } else {
      null
    }
  }

  /**
   * Convert each notification into a raw record map according to the config.
   * Standard fields are always included; additional fields defined by
   * `rawCfg.fields` are appended.
   */
  override def apply(p: Pipeline, in: PCollection[NotificationStage.Notification])(implicit ctx: PipelineCtx): PCollection[Map[String, Any]] = {
    in.apply(name, ParDo.of(new DoFn[NotificationStage.Notification, Map[String, Any]]() {
      @ProcessElement
      def proc(c: DoFn[NotificationStage.Notification, Map[String, Any]]#ProcessContext): Unit = {
        val n = c.element()
        // Base fields
        var m: Map[String, Any] = Map(
          "table" -> n.table,
          "event_type" -> n.eventType,
          "payload" -> n.raw
        )
        // Apply additional field mappings from config
        for ((dst, spec) <- rawCfg.fields) {
          m = m + (dst -> eval(n, spec))
        }
        c.output(m)
      }
    }))
  }
}
