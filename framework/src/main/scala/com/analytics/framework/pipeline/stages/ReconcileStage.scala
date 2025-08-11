package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.{BaseStage, PipelineCtx}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
final case class ReconCfg(zone:String, table:String, gcsToS3: Map[String,String])
class ReconcileStage[T <: Map[String,Any]](cfg: ReconCfg, s3Sample: () => Iterable[Map[String,Any]], log: String => Unit)
  extends BaseStage[T,T] {
  val name = s"Reconcile(${cfg.zone}.${cfg.table})"
  def apply(p: Pipeline, in: PCollection[T])(implicit ctx: PipelineCtx) = {
    in.apply(name, ParDo.of(new DoFn[T,T](){
      @ProcessElement def proc(c: DoFn[T,T]#ProcessContext): Unit = {
        val left = c.element()
        val mappedLeft = cfg.gcsToS3.map{ case (gcs,s3) => s3 -> left.getOrElse(gcs, null) }
        val awsRows = s3Sample()
        val matched = awsRows.exists { r =>
          cfg.gcsToS3.values.forall(s3c => Option(mappedLeft.getOrElse(s3c,null)).map(_.toString) ==
                                           Option(r.getOrElse(s3c,null)).map(_.toString))
        }
        val json = s"""{"table":"${cfg.table}","zone":"${cfg.zone}","matched":$matched}"""
        log(json); c.output(left)
      }
    }))
  }
}
