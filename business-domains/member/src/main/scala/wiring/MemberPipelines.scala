package com.domain.member.wiring
import com.analytics.framework.core.base.PipelineCtx
import com.analytics.framework.core.orch.StageGraph
import com.analytics.framework.pipeline.stages._
import com.analytics.framework.modules.audit.GcsAuditLogger
import com.analytics.framework.modules.quality.{RulesLoader}
import com.analytics.framework.modules.reconciliation.{ReconcileMappingLoader}
import scala.jdk.CollectionConverters._

object MemberPipelines {

  private def gcsAuditPath(ctx: PipelineCtx, kind:String, zone:String, window:String) =
    s"${ctx.buckets.getOrElse("audit","gs://demo-the-1-audit")}/${kind}/${ctx.domain}/${zone}/${window}.log"

  /** refined ingest: Transform -> BQ write -> Reconcile (with gcs_to_s3) -> DQ+Audit */
  def refinedIngest(ctx: PipelineCtx, table:String, moduleClass:String, moduleParams:Map[String,Any],
                    qualityRulesPath:String, reconcileMapPath:String): StageGraph = {
    val g = new StageGraph()
      .add(new TransformStage[Map[String,Any], Map[String,Any]](moduleClass, moduleParams))
      .add(new BqWriteStage(ctx.datasets("refined"), table))
      .add(new ReconcileStage[Map[String,Any]](
        ReconCfg(zone="refined", table=table, gcsToS3 = ReconcileMappingLoader.get(reconcileMapPath, "refined", table)),
        s3Sample = () => Iterable.empty, // TODO: plug S3 reader
        log = line => GcsAuditLogger.writeLine(gcsAuditPath(ctx,"reconcile_log","refined", ctx.options.getOrElse("window_id","now").toString), line)
      ))
      .add(new QualityStage[Map[String,Any]](
        rules = RulesLoader.loadNotNullRules(qualityRulesPath),
        log   = line => GcsAuditLogger.writeLine(gcsAuditPath(ctx,"data_quality_log","refined", ctx.options.getOrElse("window_id","now").toString), line)
      ))
    g
  }

  /** analytics ingest: Transform -> BQ write -> Reconcile -> DQ+Audit */
  def analyticsIngest(ctx: PipelineCtx, table:String, moduleClass:String, moduleParams:Map[String,Any],
                      qualityRulesPath:String, reconcileMapPath:String): StageGraph = {
    val g = new StageGraph()
      .add(new TransformStage[Map[String,Any], Map[String,Any]](moduleClass, moduleParams))
      .add(new BqWriteStage(ctx.datasets("analytics"), table))
      .add(new ReconcileStage[Map[String,Any]](
        ReconCfg(zone="analytics", table=table, gcsToS3 = ReconcileMappingLoader.get(reconcileMapPath, "analytics", table)),
        s3Sample = () => Iterable.empty,
        log = line => GcsAuditLogger.writeLine(gcsAuditPath(ctx,"reconcile_log","analytics", ctx.options.getOrElse("window_id","now").toString), line)
      ))
      .add(new QualityStage[Map[String,Any]](
        rules = RulesLoader.loadNotNullRules(qualityRulesPath),
        log   = line => GcsAuditLogger.writeLine(gcsAuditPath(ctx,"data_quality_log","analytics", ctx.options.getOrElse("window_id","now").toString), line)
      ))
    g
  }
}
