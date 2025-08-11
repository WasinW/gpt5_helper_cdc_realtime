package com.domain.member.app
import com.analytics.framework.core.base.PipelineCtx
import com.analytics.framework.core.orch.StageGraph
import com.analytics.framework.pipeline.stages._
import com.analytics.framework.modules.audit.GcsAuditLogger
import com.analytics.framework.modules.quality.{RulesLoader}
import com.analytics.framework.modules.reconciliation.{ReconcileMappingLoader}
import com.analytics.framework.utils.{YamlLoader}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.{PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.Flatten
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import scala.jdk.CollectionConverters._

object MemberMain {
  def main(args: Array[String]): Unit = {
    val params = args.sliding(2,1).collect{ case Array(k, v) if k.startsWith("--") => k.stripPrefix("--")->v }.toMap
    val configPath = params.getOrElse("config", "business-domains/member/resources/pipeline_config.yaml")
    val pipelineName = params.getOrElse("pipeline", "raw_ingest")
    val windowId = params.getOrElse("window_id", System.currentTimeMillis().toString)

    val cfg = YamlLoader.load(configPath)
    val domain = cfg.getOrElse("domain","member").toString
    val datasets = cfg.get("datasets").map(_.asInstanceOf[Map[String,String]]).getOrElse(Map(
      "raw"->"member_raw", "structure"->"member_structure", "refined"->"member_refined", "analytics"->"member_analytics"
    ))
    val buckets = Map(
      "audit" -> "gs://demo-the-1-audit"
    )

    val projectId = sys.env.getOrElse("GCP_PROJECT", "demo-the-1")
    val region    = sys.env.getOrElse("GCP_REGION",  "asia-southeast1")

    implicit val ctx: PipelineCtx = PipelineCtx(
      projectId=projectId, region=region, domain=domain,
      datasets=datasets, buckets=buckets,
      windowPattern=cfg.getOrElse("window_id_pattern","yyyyMMddHHmm").toString,
      configRoot=configPath, options=Map("window_id"->windowId)
    )

    val p = Pipeline.create(PipelineOptionsFactory.fromArgs(Array[String]():_*).create())

    pipelineName match {
      case "raw_ingest" =>
        val pubsub = cfg.get("pubsub").map(_.asInstanceOf[Map[String,String]]).getOrElse(Map.empty)
        val subCreate = pubsub.getOrElse("subscription_create","projects/demo-the-1/subscriptions/member-events-create-sub")
        val subUpdate = pubsub.getOrElse("subscription_update","projects/demo-the-1/subscriptions/member-events-update-sub")

        // read both create & update then flatten
        val srcCreate: PCollection[PubsubMessage] = NotificationStage.readFrom(p, subCreate)
        val srcUpdate: PCollection[PubsubMessage] = NotificationStage.readFrom(p, subUpdate)
        val merged: PCollection[PubsubMessage] = PCollectionList.of(srcCreate).and(srcUpdate).apply("FlattenSubs", Flatten.pCollections())

        val notif = new NotificationStage().apply(p, merged)
        val asMap = new MessageToRawStage().apply(p, notif)

        val dqRulesPath = cfg.get("quality_rules_path").map(_.toString).getOrElse("business-domains/member/resources/quality_rules.yaml")
        val dq = new QualityStage[Map[String,Any]](
          rules = RulesLoader.loadNotNullRules(dqRulesPath),
          log   = line => GcsAuditLogger.writeLine(s"${buckets("audit")}/data_quality_log/${domain}/raw/${windowId}.log", line)
        ).apply(p, asMap)

        // dynamic write to raw dataset; table = element("table")
        new BqWriteDynamicStage(datasets("raw"), m => m.getOrElse("table","unknown").toString).apply(p, dq)

        // reconcile (raw): mapping + S3 sample (TODO: plug S3)
        new ReconcileStage[Map[String,Any]](
          ReconCfg("raw", "<dynamic>", Map.empty), // จะทำราย table ในรอบถัดไป (ตอนผูก S3 จริง)
          () => Iterable.empty,
          line => GcsAuditLogger.writeLine(s"${buckets("audit")}/reconcile_log/${domain}/raw/${windowId}.log", line)
        ).apply(p, dq)

      case "raw_to_structure" =>
        // per table mapping from structure.yaml
        val mappingPath = cfg.get("structure").map(_.asInstanceOf[Map[String,Any]].getOrElse("mapping","").toString)
          .filter(_.nonEmpty).getOrElse("business-domains/member/resources/mappings/structure.yaml")
        val mp = YamlLoader.load(mappingPath) // table -> {dst: src}
        val tables = cfg.get("structure").flatMap(_.asInstanceOf[Map[String,Any]].get("tables").map(_.asInstanceOf[List[String]])).getOrElse(Nil)

        tables.foreach { t =>
          val bqR = new BqReadStage(datasets("raw"), t).apply(p, null) // stage ตัวนี้ไม่ใช้ in
          val mapCfg = mp.getOrElse(t, Map.empty[String,String]).asInstanceOf[Map[String,String]]
          val mapped = new FieldMapStage(mapCfg).apply(p, bqR)
          val written = new BqWriteStage(datasets("structure"), t).apply(p, mapped)

          // reconcile per table
          new ReconcileStage[Map[String,Any]](
            ReconCfg("structure", t, Map.empty), // โหลด mapping ราย table ในคอมมิตถัดไปพร้อม S3
            () => Iterable.empty,
            line => GcsAuditLogger.writeLine(s"${buckets("audit")}/reconcile_log/${domain}/structure/${windowId}.log", line)
          ).apply(p, written)

          new QualityStage[Map[String,Any]](
            rules = RulesLoader.loadNotNullRules("business-domains/member/resources/quality_rules.yaml"),
            log   = line => GcsAuditLogger.writeLine(s"${buckets("audit")}/data_quality_log/${domain}/structure/${windowId}.log", line)
          ).apply(p, written)
        }

      case "refined_ingest" =>
        val recPath = "business-domains/member/resources/reconcile_mapping.yaml"
        val dqPath = "business-domains/member/resources/quality_rules.yaml"
        val refinedCfg = cfg.get("refined").map(_.asInstanceOf[Map[String,Any]]).getOrElse(Map())
        val tables = refinedCfg.get("tables").map(_.asInstanceOf[List[Map[String,Any]]]).getOrElse(Nil)
        tables.foreach { t =>
          val name = t("name").toString
          val mod  = t("module").asInstanceOf[Map[String,Any]]
          val clazz= mod("class").toString
          val params = mod.getOrElse("params", Map.empty[String,Any]).asInstanceOf[Map[String,Any]]

          new com.domain.member.wiring.MemberPipelines.refinedIngest(ctx, name, clazz, params, dqPath, recPath)
        }

      case "analysis_ingest" =>
        val recPath = "business-domains/member/resources/reconcile_mapping.yaml"
        val dqPath = "business-domains/member/resources/quality_rules.yaml"
        val aCfg = cfg.get("analytics").map(_.asInstanceOf[Map[String,Any]]).getOrElse(Map())
        val tables = aCfg.get("tables").map(_.asInstanceOf[List[Map[String,Any]]]).getOrElse(Nil)
        tables.foreach { t =>
          val name = t("name").toString
          val mod  = t("module").asInstanceOf[Map[String,Any]]
          val clazz= mod("class").toString
          val params = mod.getOrElse("params", Map.empty[String,Any]).asInstanceOf[Map[String,Any]]
          new com.domain.member.wiring.MemberPipelines.analyticsIngest(ctx, name, clazz, params, dqPath, recPath)
        }

      case other =>
        throw new IllegalArgumentException(s"Unknown pipeline: $other")
    }

    p.run().waitUntilFinish()
  }
}
