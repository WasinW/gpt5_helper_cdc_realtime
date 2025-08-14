// framework/src/main/scala/com/analytics/framework/pipeline/core/WindowingStrategy.scala
package com.analytics.framework.pipeline.core

import org.apache.beam.sdk.transforms.windowing._
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Duration

object WindowingStrategy {
  
  sealed trait WindowConfig {
    def duration: Duration
    def allowedLateness: Duration
    def trigger: Trigger
  }
  
  // 5 Processing Windows Configuration
  case object NotificationWindow extends WindowConfig {
    val duration = Duration.standardSeconds(1)
    val allowedLateness = Duration.standardSeconds(10)
    val trigger = AfterWatermark.pastEndOfWindow()
  }
  
  case object RawWindow extends WindowConfig {
    val duration = Duration.standardSeconds(60)
    val allowedLateness = Duration.standardMinutes(5)
    val trigger = AfterWatermark.pastEndOfWindow()
      .withEarlyFirings(
        AfterProcessingTime.pastFirstElementInPane()
          .plusDelayOf(Duration.standardSeconds(30))
      )
      .withLateFirings(AfterPane.elementCountAtLeast(1))
  }
  
  case object StructureWindow extends WindowConfig {
    val duration = Duration.standardSeconds(60)
    val allowedLateness = Duration.standardMinutes(5)
    val trigger = AfterWatermark.pastEndOfWindow()
  }
  
  case object RefinedWindow extends WindowConfig {
    val duration = Duration.standardSeconds(300)
    val allowedLateness = Duration.standardMinutes(10)
    val trigger = AfterWatermark.pastEndOfWindow()
      .withEarlyFirings(
        AfterProcessingTime.pastFirstElementInPane()
          .plusDelayOf(Duration.standardMinutes(2))
      )
  }
  
  case object AnalyticsWindow extends WindowConfig {
    val duration = Duration.standardSeconds(300)
    val allowedLateness = Duration.standardMinutes(10)
    val trigger = AfterWatermark.pastEndOfWindow()
  }
  
  def applyWindow[T](
    input: PCollection[T],
    config: WindowConfig
  ): PCollection[T] = {
    input.apply(
      s"Window_${config.getClass.getSimpleName}",
      Window.into[T](FixedWindows.of(config.duration))
        .triggering(config.trigger)
        .withAllowedLateness(config.allowedLateness)
        .accumulatingFiredPanes()
    )
  }
}