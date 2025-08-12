package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
trait TransformModule {
  def init(params: Map[String, Any]): Unit
  def exec(in: Map[String, Any]): Map[String, Any]
}
class TransformStage[I <: Map[String,Any], O <: Map[String,Any]](clazz:String, params: Map[String,Any])
  extends BaseStage[I,O]{
  override def name: String = s"transform:$clazz"
  def apply(p:Pipeline, in:PCollection[I])(implicit ctx:PipelineCtx): PCollection[O] = {
    in.apply(name, ParDo.of(new DoFn[I,O](){
      @ProcessElement def proc(c: DoFn[I,O]#ProcessContext): Unit = {
        val mod = Class.forName(clazz).newInstance().asInstanceOf[TransformModule]
        mod.init(params)
        val out = mod.exec(c.element().asInstanceOf[Map[String,Any]])
        c.output(out.asInstanceOf[O])
      }
    }))
  }
}
