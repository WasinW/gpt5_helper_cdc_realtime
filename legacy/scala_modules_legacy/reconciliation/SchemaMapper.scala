package com.analytics.framework.modules.reconciliation

/**
  * Maps column names from the AWS schema to the BigQuery schema.
  * Configuration for mappings should be provided via YAML.  In this
  * stub the mapper performs an identity mapping.
  */
class SchemaMapper {
  def mapColumn(name: String): String = name
}