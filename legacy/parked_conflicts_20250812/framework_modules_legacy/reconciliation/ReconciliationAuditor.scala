package com.analytics.framework.modules.reconciliation

/**
  * Records reconciliation results.  In a real implementation this
  * would write metrics to GCS or BigQuery for further analysis.
  */
class ReconciliationAuditor {
  def audit(successCount: Long, failureCount: Long): Unit = {
    println(s"ReconciliationAuditor: success=$successCount failure=$failureCount")
  }
}