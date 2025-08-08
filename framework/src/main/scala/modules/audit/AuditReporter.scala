package com.analytics.framework.modules.audit

/**
  * Sends audit summaries or alerts to external systems such as
  * monitoring dashboards, Slack or email.  Left unimplemented in
  * this stub.
  */
class AuditReporter {
  def report(summary: String): Unit = {
    println(s"AuditReporter: reporting summary: $summary")
  }
}