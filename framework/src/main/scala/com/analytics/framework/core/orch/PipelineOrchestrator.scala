package com.analytics.framework.core.orch
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
class PipelineOrchestrator(graph: StageGraph) extends Serializable {
  def run[I](p: Pipeline, source: PCollection[I])(implicit ctx: PipelineCtx): Unit = {
    var cur: PCollection[_] = source
    graph.stages.foreach { st =>
      cur = st.asInstanceOf[BaseStage[Any,Any]].apply(p, cur.asInstanceOf[PCollection[Any]])
    }
  }
}
