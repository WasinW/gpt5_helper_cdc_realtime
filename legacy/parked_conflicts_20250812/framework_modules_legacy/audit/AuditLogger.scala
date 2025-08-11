package com.analytics.framework.modules.audit

/**
  * Top‑level entry point for producing audit events.  An instance
  * of [[AuditStorage]] is used to persist structured logs.  The
  * [[AuditReporter]] can be used to send alerts or summaries to
  * monitoring systems.
  */
class AuditLogger(storage: AuditStorage) {
  def logPipelineCompletion(domain: String, ingestionType: String): Unit = {
    storage.write(Map(
      "event" -> "pipeline_complete",
      "domain" -> domain,
      "ingestionType" -> ingestionType,
      "timestamp" -> System.currentTimeMillis()
    ))
  }
}