package com.analytics.framework.core.base
case class ReconcileResult(countMatch: Boolean, diffs: List[Map[String,String]])
trait ReconcileStrategy[L, R] extends Serializable {
  def compare(left: L, right: R, ctx: PipelineCtx): ReconcileResult
}
