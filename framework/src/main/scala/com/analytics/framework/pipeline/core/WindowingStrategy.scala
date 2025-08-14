package com.analytics.framework.pipeline.core

import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Duration
import com.analytics.framework.utils.YamlLoader
import scala.collection.JavaConverters._

object WindowingStrategy {
  
  case class WindowConfig(
    durationSec: Int,
    allowedLatenessSec: Int,
    earlyFiringSec: Option[Int] = None,
    triggerType: String = "AfterWatermark"
  ) {
    def duration: Duration = Duration.standardSeconds(durationSec)
    def allowedLateness: Duration = Duration.standardSeconds(allowedLatenessSec)
    
    def trigger: Trigger = triggerType match {
      case "AfterWatermarkWithEarlyFiring" if earlyFiringSec.isDefined =>
        AfterWatermark.pastEndOfWindow()
          .withEarlyFirings(
            AfterProcessingTime.pastFirstElementInPane()
              .plusDelayOf(Duration.standardSeconds(earlyFiringSec.get))
          )
      case _ => 
        AfterWatermark.pastEndOfWindow()
    }
  }
  
  def loadWindowConfig(configPath: String, stage: String): WindowConfig = {
    val cfg = YamlLoader.load(configPath)
    val windowing = cfg.get("windowing").map(_.asInstanceOf[Map[String,Any]]).getOrElse(Map.empty)
    val defaults = windowing.get("defaults").map(_.asInstanceOf[Map[String,Any]]).getOrElse(Map.empty)
    val stageConfig = defaults.get(stage).map(_.asInstanceOf[Map[String,Any]]).getOrElse(Map.empty)
    
    WindowConfig(
      durationSec = stageConfig.get("duration_sec").map(_.asInstanceOf[Int]).getOrElse(60),
      allowedLatenessSec = stageConfig.get("allowed_lateness_sec")
        .map(_.asInstanceOf[Int])
        .orElse(stageConfig.get("allowed_lateness_min").map(_.asInstanceOf[Int] * 60))
        .getOrElse(300),
      earlyFiringSec = stageConfig.get("early_firing_sec").map(_.asInstanceOf[Int]),
      triggerType = stageConfig.get("trigger_type").map(_.toString).getOrElse("AfterWatermark")
    )
  }
  
  def applyWindow[T](input: PCollection[T], config: WindowConfig, name: String): PCollection[T] = {
    input.apply(
      s"Window_$name",
      Window.into[T](FixedWindows.of(config.duration))
        .triggering(config.trigger)
        .withAllowedLateness(config.allowedLateness)
        .accumulatingFiredPanes()
    )
  }
}
