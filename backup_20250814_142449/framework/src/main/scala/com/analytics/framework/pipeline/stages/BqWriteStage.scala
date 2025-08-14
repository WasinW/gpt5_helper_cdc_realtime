package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.PipelineCtx

class BqWriteStage(dataset: String, table: String) extends BaseStage[Map[String, Any], Map[String, Any]] {
  override def name: String = s"BqWriteStage($dataset.$table)"
  override def run(ctx: PipelineCtx, in: Seq[Map[String, Any]]): Seq[Map[String, Any]] = {
    println(s"[BQ WRITE] ${in.size} rows -> $dataset.$table")
    in
  }
}
