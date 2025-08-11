package com.analytics.framework.modules.quality
import com.analytics.framework.core.base.{QualityRule, PipelineCtx}
class NotNullRule(col:String) extends QualityRule[Map[String,Any]] {
  val id = s"not_null:$col"
  def check(row: Map[String,Any], ctx: PipelineCtx): Boolean = row.get(col).exists(_ != null)
}
