package com.analytics.framework.pipeline.stages
import com.analytics.framework.utils.JavaInterop
import scala.collection.JavaConverters._

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import com.analytics.framework.core.base.PipelineCtx
import scala.collection.JavaConverters._

class QualityStage[T <: Map[String,Any]](rules: QualityRules, audit: String => Unit) {

  def apply(p: Pipeline, in: PCollection[Map[String,Any]])(implicit ctx: PipelineCtx): PCollection[Map[String,Any]] = {
    val notNullCols = rules.notNull.getOrElse(Nil)
    in.apply("DQ NotNull", ParDo.of(new DoFn[Map[String,Any], Map[String,Any]] {
      @ProcessElement
      def proc(c: DoFn[Map[String,Any], Map[String,Any]]#ProcessContext): Unit = {
        val m = c.element()
        val fails = notNullCols.flatMap(col => if (!m.contains(col) || m(col) == null) Some(col) else None)
        if (fails.nonEmpty) {
          val line = s"""{"level":"WARN","type":"not_null","cols":"${fails.mkString(",")}","row":"$m"}"""
          audit(line)
        }
        c.output(m)
      }
    }))
  }
}

case class QualityRules(notNull: Option[List[String]])
object RulesLoader {
  def loadNotNullRules(path:String): QualityRules = {
    val in = new java.io.FileInputStream(path)
    val yaml = new org.yaml.snakeyaml.Yaml()
    val data: Map[String,Any] = in match {
        case m: Map[_, _]            => m.asInstanceOf[Map[String,Any]]
        case jm: java.util.Map[_, _] => JavaInterop.deepMap(jm)
        case other                    => Map("value" -> other)
      }
    val nn = Option(data.get("not_null")).map(_.asInstanceOf[java.util.List[String]].asScala.toList).getOrElse(Nil)
    QualityRules(Some(nn))
  }
}
