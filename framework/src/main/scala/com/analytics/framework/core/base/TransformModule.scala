package com.analytics.framework.core.base
trait TransformModule[I,O] extends Serializable {
  def init(config: Map[String, Any], ctx: PipelineCtx): Unit
  def exec(input: I, ctx: PipelineCtx): Iterable[O]
  def close(): Unit = ()
}
