package com.analytics.framework.pipeline.core
import com.analytics.framework.core.base.PipelineCtx
import com.analytics.framework.utils.YamlLoader
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.windowing.{AfterProcessingTime, Repeatedly}
import org.joda.time.Duration
import scala.collection.JavaConverters._
class PipelineOrchestrator(implicit ctx: PipelineCtx){
  def build(): Pipeline = {
    val opts: PipelineOptions = PipelineOptionsFactory.create()
    Pipeline.create(opts)
  }
  def readMap(path:String): Map[String, Map[String,String]] = {
    val m = YamlLoader.load(path)
    m.get("mapping") match {
      case Some(mm: java.util.Map[_,_]) =>
        mm.asInstanceOf[java.util.Map[String,java.util.Map[String,String]]]
          .asScala.map{case (k,v) => k -> v.asScala.toMap}.toMap
      case _ => Map.empty
    }
  }
  def triggerExample() = Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(500)))
}
