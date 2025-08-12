package com.analytics.framework.app

import com.analytics.framework.utils.{JavaInterop, YamlLoader}
import scala.collection.JavaConverters._
import com.analytics.framework.core.base.PipelineCtx
import com.analytics.framework.modules.audit.GcsAuditLogger
import com.analytics.framework.modules.quality.RulesLoader
import com.analytics.framework.pipeline.stages._
import com.analytics.framework.utils.{YamlLoader, RawConfigLoader}
import org.apache.beam.sdk.Pipeline
object CommonRunner{
  private def toStringMap(x: Any): Map[String, String] = x match {
    case m: Map[_, _] =>
      m.asInstanceOf[Map[String, Any]].map { case (k, v) => k.toString -> String.valueOf(v) }
    case jm: java.util.Map[_, _] =>
      jm.asInstanceOf[java.util.Map[String, Any]].asScala.toMap.map { case (k, v) => k.toString -> String.valueOf(v) }
    case _ => Map.empty[String, String]
  }

  def main(args:Array[String]): Unit = {
    val projectId  = sys.props.getOrElse("projectId","demo")
    val region     = sys.props.getOrElse("region","asia-southeast1")
    val domain     = sys.props.getOrElse("domain","member")
    val windowId   = sys.props.getOrElse("window_id","202501010000")
    val configPath = "business-domains/member/resources/pipeline_config.yaml"
    val cfg: Map[String, Any] = YamlLoader.load(configPath) // ได้ Scala Map แล้ว
    val datasets: Map[String, String] =
      cfg.get("datasets")
        .map(JavaInterop.deepAsScala)
        .map(toStringMap)
        .getOrElse(Map.empty)

    val buckets: Map[String, String] =
      cfg.get("buckets")
        .map(JavaInterop.deepAsScala)
        .map(toStringMap)
        .getOrElse(Map("audit" -> "gs://dummy-bucket"))

    implicit val ctx: PipelineCtx =
      PipelineCtx(projectId, region, domain, datasets, buckets, cfg.getOrElse("window_id_pattern","yyyyMMddHHmm").toString, configPath, Map("window_id"->windowId))
    def audit(kind:String, zone:String) = (line:String) => GcsAuditLogger.writeLine(s"${buckets("audit")}/" + s"$kind/$domain/$zone/$windowId.log", line)
    val p = Pipeline.create()
    // ตัวอย่างสาย raw (สั้นๆ): ปล่อยผ่านเฉย ๆ เพื่อคอมไพล์
    // val _ = (new QualityStage[Map[String,Any]](RulesLoader.loadNotNullRules("business-domains/member/resources/quality_rules.yaml"), audit("data_quality_log","raw")))
    // val _qualityLink =
    //   new QualityStage[Map[String,Any]](
    //     QualityRules(RulesLoader.loadNotNullRules("business-domains/member/resources/quality_rules.yaml")),
    //     audit("data_quality_log","raw")
    //   )
    val _qualityLink =
      new QualityStage[Map[String,Any]](
        QualityRules(notNull = Some(RulesLoader.loadNotNullRules("business-domains/member/resources/quality_rules.yaml"))),
        audit("data_quality_log","raw")
      )

    // ตัวอย่าง refined/analytics wiring (สั้น)
    val _2 = (new BqWriteStage(datasets("refined"), "member_profile"))
    // val _3 = (new ReconcileStage[Map[String,Any]](ReconCfg("refined","member_profile", Map.empty), Nil, audit("reconcile_log","refined")))
    val _recLink =
      new ReconcileStage[Map[String,Any]]("refined","member_profile", Map.empty, Nil, audit("reconcile_log","refined"))

    p.run().waitUntilFinish()
  }
}
