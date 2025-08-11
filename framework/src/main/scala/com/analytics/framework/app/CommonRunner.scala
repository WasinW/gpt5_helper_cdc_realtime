package com.analytics.framework.app
import com.analytics.framework.core.base.PipelineCtx
import com.analytics.framework.pipeline.stages._
import com.analytics.framework.modules.audit.GcsAuditLogger
import com.analytics.framework.modules.quality.RulesLoader
import com.analytics.framework.modules.reconciliation.{ReconcileMappingLoader, S3SampleProvider}
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

        // Per-table reconcile using S3 samples
        val recMap = ReconcileMappingLoader.load("business-domains/member/resources/reconcile_mapping.yaml")
        rawCfg.allowedTables.foreach { t =>
          val filtered = new FilterByTableStage(t).apply(p, dq)
          val g2s = recMap.getOrElse("raw", Map.empty).getOrElse(t, Map.empty)
          val sample = S3SampleProvider.build(projectId, cfg, "raw", t, windowId, g2s)
          new ReconcileStage[Map[String,Any]](ReconCfg("raw", t, g2s), sample, audit("reconcile_log","raw")).apply(p, filtered)
        }

      case "raw_to_structure" =>
        val struct   = cfg.get("structure").map(_.asInstanceOf[Map[String,Any]]).getOrElse(Map.empty)
        val tables   = struct.get("tables").map(_.asInstanceOf[List[String]]).getOrElse(Nil)
        val mapPath  = struct.get("mapping").map(_.toString).getOrElse("business-domains/member/resources/mappings/structure.yaml")
        val mp       = YamlLoader.load(mapPath).asInstanceOf[Map[String, Map[String,String]]]
        val recMap   = ReconcileMappingLoader.load("business-domains/member/resources/reconcile_mapping.yaml")

        tables.foreach { t =>
          val r   = new BqReadStage(datasets("raw"), t).apply(p, null)
          val out = new FieldMapStage(mp.getOrElse(t, Map.empty)).apply(p, r)
          val w   = new BqWriteStage(datasets("structure"), t).apply(p, out)

          val g2s = recMap.getOrElse("structure", Map.empty).getOrElse(t, Map.empty)
          val sample = S3SampleProvider.build(projectId, cfg, "structure", t, windowId, g2s)
          new ReconcileStage[Map[String,Any]](ReconCfg("structure", t, g2s), sample, audit("reconcile_log","structure")).apply(p, w)
          new QualityStage[Map[String,Any]](RulesLoader.loadNotNullRules("business-domains/member/resources/quality_rules.yaml"), audit("data_quality_log","structure")).apply(p, w)
        }

      case "refined_ingest" =>
        val recPath = "business-domains/member/resources/reconcile_mapping.yaml"; val dqPath = "business-domains/member/resources/quality_rules.yaml"
        val cfgR = cfg.get("refined").map(_.asInstanceOf[Map[String,Any]]).getOrElse(Map())
        val tables = cfgR.get("tables").map(_.asInstanceOf[List[Map[String,Any]]]).getOrElse(Nil)
        tables.foreach { t =>
          val name   = t("name").toString
          val module = t("module").asInstanceOf[Map[String,Any]]; val clazz = module("class").toString
          val inputs = t.get("inputs").map(_.asInstanceOf[List[String]]).getOrElse(Nil)

          // read & flatten inputs from STRUCTURE dataset
          val collList = inputs.map(tbl => new BqReadStage(datasets("structure"), tbl).apply(p, null))
          val merged = if (collList.size == 1) collList.head else PCollectionList.of(collList:_*).apply(s"Flatten-$name", Flatten.pCollections())

          val out    = new TransformStage[Map[String,Any], Map[String,Any]](clazz, module.getOrElse("params", Map.empty[String,Any]).asInstanceOf[Map[String,Any]]).apply(p, merged)
          val w      = new BqWriteStage(datasets("refined"), name).apply(p, out)

          val g2s = ReconcileMappingLoader.get(recPath, "refined", name)
          val sample = S3SampleProvider.build(projectId, cfg, "refined", name, windowId, g2s)
          new ReconcileStage[Map[String,Any]](ReconCfg("refined", name, g2s), sample, audit("reconcile_log","refined")).apply(p, w)
          new QualityStage[Map[String,Any]](RulesLoader.loadNotNullRules(dqPath), audit("data_quality_log","refined")).apply(p, w)
        }

      case "analysis_ingest" =>
        val recPath = "business-domains/member/resources/reconcile_mapping.yaml"; val dqPath = "business-domains/member/resources/quality_rules.yaml"
        val cfgA = cfg.get("analytics").map(_.asInstanceOf[Map[String,Any]]).getOrElse(Map())
        val tables = cfgA.get("tables").map(_.asInstanceOf[List[Map[String,Any]]]).getOrElse(Nil)
        tables.foreach { t =>
          val name   = t("name").toString
          val module = t("module").asInstanceOf[Map[String,Any]]; val clazz = module("class").toString
          val inputs = t.get("inputs").map(_.asInstanceOf[List[String]]).getOrElse(Nil)

          val collList = inputs.map(tbl => new BqReadStage(datasets("refined"), tbl).apply(p, null))
          val merged = if (collList.size == 1) collList.head else PCollectionList.of(collList:_*).apply(s"Flatten-$name", Flatten.pCollections())

          val out    = new TransformStage[Map[String,Any], Map[String,Any]](clazz, module.getOrElse("params", Map.empty[String,Any]).asInstanceOf[Map[String,Any]]).apply(p, merged)
          val w      = new BqWriteStage(datasets("analytics"), name).apply(p, out)

          val g2s = ReconcileMappingLoader.get(recPath, "analytics", name)
          val sample = S3SampleProvider.build(projectId, cfg, "analytics", name, windowId, g2s)
          new ReconcileStage[Map[String,Any]](ReconCfg("analytics", name, g2s), sample, audit("reconcile_log","analytics")).apply(p, w)
          new QualityStage[Map[String,Any]](RulesLoader.loadNotNullRules(dqPath), audit("data_quality_log","analytics")).apply(p, w)
        }

      case other => throw new IllegalArgumentException(s"Unknown pipeline: $other")
    }
    p.run().waitUntilFinish()
  }
}
