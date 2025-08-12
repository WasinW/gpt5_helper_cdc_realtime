package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.PipelineCtx
import com.analytics.framework.connectors.S3JsonlReader

case class ReconCfg(zone: String, table: String, options: Map[String, String])

class ReconcileStage[T <: Map[String, Any]](
  cfg: ReconCfg,
  primaryKeys: List[String],
  audit: String => Unit
) extends BaseStage[T, T] {
  // >>> เพิ่มคอนสตรัคเตอร์นี้ เพื่อให้โค้ดเดิมที่เรียก new ReconcileStage("refined", "member_profile", Map.empty, Nil, audit(...))
  def this(zone: String, table: String, options: Map[String, String], primaryKeys: List[String], audit: String => Unit) =
    this(ReconCfg(zone, table, options), primaryKeys, audit)

  override def name: String = "ReconcileStage"
  override def run(ctx: PipelineCtx, in: Seq[T]): Seq[T] = {
    val s3uri  = cfg.options.getOrElse("s3_uri", "")
    val ref    = if (s3uri.nonEmpty) S3JsonlReader.readJsonLines(s3uri) else Seq.empty[Map[String, Any]]
    val refIdx = ref.groupBy(r => primaryKeys.map(k => r.getOrElse(k, null)).mkString("||"))
    in.foreach { rec =>
      val key = primaryKeys.map(k => rec.getOrElse(k, null)).mkString("||")
      if (!refIdx.contains(key)) audit(s"RECON_MISSING:$key")
    }
    in
  }
}
