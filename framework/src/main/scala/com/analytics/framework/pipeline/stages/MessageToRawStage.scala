package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.PCollection
import scala.util.Try

class MessageToRawStage extends BaseStage[NotificationStage.Notification, Map[String,Any]] {
  val name = "MessageToRaw"
  def apply(p: Pipeline, in: PCollection[NotificationStage.Notification])(implicit ctx: PipelineCtx): PCollection[Map[String,Any]] = {
    in.apply(name, ParDo.of(new DoFn[NotificationStage.Notification, Map[String,Any]](){
      @ProcessElement def proc(c: DoFn[NotificationStage.Notification, Map[String,Any]]#ProcessContext): Unit = {
        val n = c.element()
        // เก็บ raw json ทั้งก้อน + คีย์หลักพื้นฐาน; framework ไม่ตีความโครงสร้างโดเมน
        val m: Map[String,Any] = Map(
          "account_id" -> n.accountId,
          "event_type" -> n.eventType,
          "raw_payload" -> n.raw,
          "ingest_ts" -> System.currentTimeMillis().toString
        )
        // ระบุปลายทาง table ด้วย key "table"
        val withTable = m + ("table" -> (if (n.table==null || n.table.isEmpty) "unknown" else n.table))
        c.output(withTable)
      }
    }))
  }
}
