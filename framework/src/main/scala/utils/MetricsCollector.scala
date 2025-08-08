package com.analytics.framework.utils

/**
  * Collects and reports pipeline metrics.  In a real system you
  * would integrate with Cloud Monitoring or another metrics backend.
  * Here we simply print out metric names and values.
  */
class MetricsCollector(projectId: String, pipelineId: String) {
  def recordMetric(name: String, value: Double): Unit = {
    println(s"MetricsCollector: [$pipelineId] $name = $value")
  }
}