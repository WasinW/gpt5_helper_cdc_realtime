package com.analytics.framework.app
import com.analytics.framework.core.base.PipelineCtx
import com.analytics.framework.pipeline.stages._
import com.analytics.framework.modules.audit.GcsAuditLogger
import com.analytics.framework.modules.quality.RulesLoader
import com.analytics.framework.utils.YamlLoader
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Flatten
import org.apache.beam.sdk.values.{PCollection, PCollectionList}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import scala.jdk.CollectionConverters._

object CommonRunner {
  def main(args: Array[String]): Unit = {
    val params = args.sliding(2,1).collect{ case Array(k, v) if k.startsWith("--") => k.stripPrefix("--")->v }.toMap
    val configPath  = params.getOrElse("config", "business-domains/member/resources/pipeline_config.yaml")
    val pipeline    = params.getOrElse("pipeline", "raw_ingest")
    val windowId    = params.getOrElse("window_id", System.currentTimeMillis().toString)

    val cfg         = YamlLoader.load(configPath)
    val domain      = cfg.getOrElse("domain","member").toString
    val datasets    = cfg.get("datasets").map(_.asInstanceOf[Map[String,String]]).getOrElse(Map())
    val buckets     = Map("audit" -> "gs://demo-the-1-audit")
    val projectId   = sys.env.getOrElse("GCP_PROJECT", "demo-the-1")
    val region      = sys.env.getOrElse("GCP_REGION",  "asia-southeast1")

    implicit val ctx: PipelineCtx = PipelineCtx(projectId, region, domain, datasets, buckets, cfg.getOrElse("window_id_pattern","yyyyMMddHHmm").toString, configPath, Map("window_id"->windowId))
    val p = Pipeline.create(PipelineOptionsFactory.fromArgs(Array[String]():_*).create())
    def audit(kind:String, zone:String) = (line:String) => GcsAuditLogger.writeLine(s"${buckets("audit")}/" + s"$kind/$domain/$zone/$windowId.log", line)

    pipeline match {
      case "raw_ingest" =>
        val pubsub = cfg.get("pubsub").map(_.asInstanceOf[Map[String,String]]).getOrElse(Map.empty)
        val subCreate = pubsub.getOrElse("subscription_create","")
        val subUpdate = pubsub.getOrElse("subscription_update","")
        val srcCreate: PCollection[PubsubMessage] = NotificationStage.readFrom(p, subCreate)
        val srcUpdate: PCollection[PubsubMessage] = NotificationStage.readFrom(p, subUpdate)
        val merged: PCollection[PubsubMessage]    = PCollectionList.of(srcCreate).and(srcUpdate).apply("FlattenSubs", Flatten.pCollections())
        val notif = new NotificationStage().apply(p, merged)
        val rawCfg = RawConfigLoader.load(configPath)
        val mapped = new MessageToRawStage(rawCfg).apply(p, notif)
        val dq     = new QualityStage[Map[String,Any]](RulesLoader.loadNotNullRules("business-domains/member/resources/quality_rules.yaml"), audit("data_quality_log","raw")).apply(p, mapped)
        new BqWriteDynamicStage(datasets("raw"), m => m.getOrElse("table","unknown").toString).apply(p, dq)
        new ReconcileStage[Map[String,Any]](ReconCfg("raw","<dynamic>", Map.empty), () => Iterable.empty, audit("reconcile_log","raw")).apply(p, dq)

      case "raw_to_structure" =>
        val struct   = cfg.get("structure").map(_.asInstanceOf[Map[String,Any]]).getOrElse(Map.empty)
        val tables   = struct.get("tables").map(_.asInstanceOf[List[String]]).getOrElse(Nil)
        val mapPath  = struct.get("mapping").map(_.toString).getOrElse("business-domains/member/resources/mappings/structure.yaml")
        val mp       = YamlLoader.load(mapPath).asInstanceOf[Map[String, Map[String,String]]]
        tables.foreach { t =>
          val r   = new BqReadStage(datasets("raw"), t).apply(p, null)
          val out = new FieldMapStage(mp.getOrElse(t, Map.empty)).apply(p, r)
          val w   = new BqWriteStage(datasets("structure"), t).apply(p, out)
          new ReconcileStage[Map[String,Any]](ReconCfg("structure", t, Map.empty), () => Iterable.empty, audit("reconcile_log","structure")).apply(p, w)
          new QualityStage[Map[String,Any]](RulesLoader.loadNotNullRules("business-domains/member/resources/quality_rules.yaml"), audit("data_quality_log","structure")).apply(p, w)
        }

      case "refined_ingest" =>
        throw new UnsupportedOperationException("refined_ingest: ต้องระบุแหล่งอินพุตใน config (เช่น source จาก structure หรือ tech persona); เดี๋ยวผมใส่ให้ทันทีที่ได้สเปกอินพุต")

      case "analysis_ingest" =>
        throw new UnsupportedOperationException("analysis_ingest: ต้องระบุแหล่งอินพุตใน config (เช่น refined tables ที่ join); เดี๋ยวผมใส่ให้ทันทีที่ได้สเปกอินพุต")

      case other => throw new IllegalArgumentException(s"Unknown pipeline: $other")
    }
    p.run().waitUntilFinish()
  }
}
