package com.domain.member.modules
import com.analytics.framework.core.base.{TransformModule, PipelineCtx}
class MemberProfileTransform extends TransformModule[Map[String,Any], Map[String,Any]] {
  private var cfg: Map[String,Any] = Map.empty
  def init(config: Map[String, Any], ctx: PipelineCtx): Unit = { cfg = config }
  def exec(in: Map[String,Any], ctx: PipelineCtx): Iterable[Map[String,Any]] = List(in)
}
