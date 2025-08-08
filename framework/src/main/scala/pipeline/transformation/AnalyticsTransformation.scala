package com.analytics.framework.pipeline.transformation

/**
  * Generic transformation for analytics zone.  Aggregations,
  * calculations and other high‑level metrics should be implemented
  * here.  In this stub we simply pass through the input.
  */
class AnalyticsTransformation {
  def transform(refinedRecords: Any): Any = {
    println("AnalyticsTransformation: computing analytics metrics")
    refinedRecords
  }
}