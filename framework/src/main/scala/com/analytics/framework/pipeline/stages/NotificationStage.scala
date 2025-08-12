package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import scala.collection.JavaConverters._
object NotificationStage{
  case class Notification(accountId:String, eventType:String, table:String, raw:String, attrs: Map[String,String])
}
class NotificationStage() extends BaseStage[PubsubMessage, NotificationStage.Notification]{
  override def name: String = "notification"
  def apply(p: Pipeline, in: PCollection[PubsubMessage])(implicit ctx: PipelineCtx) =
    in.apply(name, ParDo.of(new DoFn[PubsubMessage, NotificationStage.Notification](){
      @ProcessElement def proc(c: DoFn[PubsubMessage, NotificationStage.Notification]#ProcessContext): Unit = {
        val msg   = c.element()
        val attrs = msg.getAttributeMap.asScala.map{ case (k,v) => (k, String.valueOf(v)) }.toMap
        val table = attrs.getOrElse("table_name","")
        val et    = attrs.getOrElse("event_type","")
        val acct  = attrs.getOrElse("account_id","")
        val raw   = new String(msg.getPayload, java.nio.charset.StandardCharsets.UTF_8)
        c.output(NotificationStage.Notification(acct, et, table, raw, attrs))
      }
    }))
}
