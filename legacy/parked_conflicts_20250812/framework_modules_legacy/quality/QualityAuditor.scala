package com.analytics.framework.modules.quality

/**
  * Records the outcome of data quality checks.  In a real pipeline
  * this might write to BigQuery or Cloud Logging.  Here it simply
  * prints the result.
  */
class QualityAuditor {
  def audit(passed: Boolean): Unit = {
    println(s"QualityAuditor: passed=$passed")
  }
}