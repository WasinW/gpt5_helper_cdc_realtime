package com.analytics.framework.pipeline.core

import org.apache.beam.sdk.transforms.windowing.{FixedWindows, SlidingWindows, Window}
import org.joda.time.Duration

/**
  * Helper class to configure Beam windowing.  You can customise the
  * window size and triggers here to tune latency and throughput.
  */
object WindowManager {
  /**
    * Returns a fixed windowing strategy of the given size in seconds.
    */
  def fixedWindow(seconds: Int) =
    Window.into(FixedWindows.of(Duration.standardSeconds(seconds)))

  /**
    * Returns a sliding window strategy.  Useful for continuously
    * aggregating overlapping intervals.
    */
  def slidingWindow(sizeSeconds: Int, periodSeconds: Int) =
    Window.into(SlidingWindows.of(Duration.standardSeconds(sizeSeconds))
      .every(Duration.standardSeconds(periodSeconds)))
}