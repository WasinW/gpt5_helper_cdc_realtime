package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.PipelineCtx
import com.analytics.framework.modules.quality.RulesLoader.Rule

class QualityStage[T](rules: List[Rule[T]], audit: String => Unit) extends BaseStage[T, T] {
  override def name: String = "QualityStage"
  override def run(ctx: PipelineCtx, in: Seq[T]): Seq[T] = {
    in.foreach { rec =>
      val fails = rules.flatMap(r => r(rec))
      if (fails.nonEmpty) audit(fails.mkString("|"))
    }
    in
  }
}
