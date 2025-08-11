package com.analytics.framework.core.orch
import com.analytics.framework.core.base.BaseStage
final class StageGraph extends Serializable {
  private val chain = scala.collection.mutable.ArrayBuffer[BaseStage[_,_]]()
  def add[I,O](s: BaseStage[I,O]): StageGraph = { chain += s; this }
  def stages: Seq[BaseStage[_,_]] = chain.toSeq
}
