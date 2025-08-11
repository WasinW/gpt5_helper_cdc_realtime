package com.analytics.framework.core.base
trait QualityRule[T] extends Serializable {
  def id: String
  def check(row: T, ctx: PipelineCtx): Boolean
  def detailIfFailed(row: T): Map[String,String] = Map.empty
}
