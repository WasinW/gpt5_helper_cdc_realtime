package com.analytics.framework.utils

/**
  * Stores and retrieves table schemas.  In a production environment
  * this could connect to a schema registry or inspect BigQuery
  * metadata.  For this scaffold we simply return an empty schema.
  */
object SchemaRegistry {
  def getSchema(table: String): Map[String, String] = {
    println(s"SchemaRegistry: retrieving schema for $table")
    Map.empty
  }
}