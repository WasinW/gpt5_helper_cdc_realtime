package com.domain.member.wiring
import com.analytics.framework.core.base.PipelineCtx
import com.analytics.framework.core.orch.{StageGraph, PipelineOrchestrator}
import com.analytics.framework.pipeline.stages._
import com.analytics.framework.modules.audit.GcsAuditLogger
import scala.io.Source
import scala.util.Try
import scala.collection.mutable

object MemberPipelines {

  // โหลด mapping GCS↔S3 ต่อ table/zone จาก YAML แบบง่าย ๆ (สมมุติเป็น key-value JSON-like)
  private def loadReconcileMap(cfgPath:String, zone:String, table:String): Map[String,String] = {
    // ในโปรดักชันแนะนำใช้ snakeyaml/jackson; ตรงนี้ทำให้เห็นภาพเฉย ๆ
    // รูปแบบ: map ของ gcs_to_s3
    Map.empty[String,String]
  }

  private def gcsAuditPath(ctx: PipelineCtx, kind:String, zone:String, window:String) =
    s"${ctx.buckets.getOrElse(\"audit\",\"demo-the-1-audit\")}/${kind}/${ctx.domain}/${zone}/${window}.log"

  def refinedIngest(ctx: PipelineCtx, table:String, moduleClass:String, moduleParams:Map[String,Any]): StageGraph = {
    val g = new StageGraph()
      // ที่นี่ควรมี Read จาก tech/refined หรือ BQ/RAW แล้วแต่กรณีจริง
      .add(new TransformStage[Map[String,Any], Map[String,Any]](moduleClass, moduleParams))
      .add(new BqWriteStage(ctx.datasets("refined"), table))
      .add(new ReconcileStage[Map[String,Any]](
        ReconCfg(zone="refined", table=table, gcsToS3 = Map.empty /* loadReconcileMap(...) */),
        s3Sample = () => Iterable.empty,       // TODO: ต่อ S3 reader จริง
        log = line => GcsAuditLogger.writeLine(gcsAuditPath(ctx,"reconcile_log","refined", ctx.options.getOrElse("window_id","now").toString), line)
      ))
      .add(new QualityStage[Map[String,Any]](
        rules = List.empty,                     // TODO: สร้าง rules จาก YAML
        log   = line => GcsAuditLogger.writeLine(gcsAuditPath(ctx,"data_quality_log","refined", ctx.options.getOrElse("window_id","now").toString), line)
      ))
    g
  }

  def analyticsIngest(ctx: PipelineCtx, table:String, moduleClass:String, moduleParams:Map[String,Any]): StageGraph = {
    val g = new StageGraph()
      .add(new TransformStage[Map[String,Any], Map[String,Any]](moduleClass, moduleParams))
      .add(new BqWriteStage(ctx.datasets("analytics"), table))
      .add(new ReconcileStage[Map[String,Any]](
        ReconCfg(zone="analytics", table=table, gcsToS3 = Map.empty /* loadReconcileMap(...) */),
        s3Sample = () => Iterable.empty,
        log = line => GcsAuditLogger.writeLine(gcsAuditPath(ctx,"reconcile_log","analytics", ctx.options.getOrElse("window_id","now").toString), line)
      ))
      .add(new QualityStage[Map[String,Any]](
        rules = List.empty,
        log   = line => GcsAuditLogger.writeLine(gcsAuditPath(ctx,"data_quality_log","analytics", ctx.options.getOrElse("window_id","now").toString), line)
      ))
    g
  }
}
