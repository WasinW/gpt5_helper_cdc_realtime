package com.analytics.framework.modules.reconciliation

final class SchemaMapper(tableMappings: Map[String, Map[String, String]]) {
  def mapAwsToGcp(table: String, row: Map[String, Any]): Map[String, Any] = {
    val m = tableMappings.getOrElse(table, Map.empty)
    row.map { case (awsCol, v) => m.getOrElse(awsCol, awsCol) -> v }
  }
}