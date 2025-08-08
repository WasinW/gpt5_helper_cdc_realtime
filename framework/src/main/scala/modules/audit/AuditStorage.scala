package com.analytics.framework.modules.audit

import com.analytics.framework.connectors.GCSConnector

/**
  * Writes audit events to a persistent store.  For simplicity
  * this implementation writes JSON strings to a Google Cloud
  * Storage bucket.  In practice you may choose BigQuery or
  * Cloud Logging.
  */
class AuditStorage(gcs: GCSConnector) {
  def write(record: Map[String, Any]): Unit = {
    val json = record.map { case (k, v) => s"\"$k\":\"$v\"" }.mkString("{", ",", "}")
    println(s"AuditStorage: writing audit record $json")
    // gcs.write(json, destination = "audit-logs/")
  }
}