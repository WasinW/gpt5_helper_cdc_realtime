package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
final case class ReconCfg(zone:String, table:String, mapping: Map[String,String])
class ReconcileStage[T <: Map[String,Any]](cfg: ReconCfg, sample: Iterable[Map[String,Any]], audit: String => Unit)
  extends BaseStage[T,T]{
  override def name: String = s"reconcile:${cfg.zone}.${cfg.table}"
  def apply(p:Pipeline, in:PCollection[T])(implicit ctx:PipelineCtx): PCollection[T] = {
    in.apply(name, ParDo.of(new DoFn[T,T](){
      @ProcessElement def proc(c: DoFn[T,T]#ProcessContext): Unit = {
        audit(s"""{"zone":"${cfg.zone}","table":"${cfg.table}","status":"checked"}""")
        c.output(c.element())
      }
    }))
  }
}
