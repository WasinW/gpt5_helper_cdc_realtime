package com.analytics.framework.pipeline.model
case class Notification(messageId: String, eventType: String, accountId: String, ts: String)
case class CDCRecord(recordId: String, op: String, data: java.util.Map[String, AnyRef], tableName: String, zone: String)
