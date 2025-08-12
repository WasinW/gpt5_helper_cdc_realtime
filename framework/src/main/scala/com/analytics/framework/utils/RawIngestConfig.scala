package com.analytics.framework.utils

/** Minimal + defensive config holder for raw ingest mapping.
  * It provides multiple aliases so that existing code keeps compiling:
  *  - mapping, specs, fields (all are Map[String,String] aliases)
  *  - get, contains helpers
  * Optional knobs (with defaults) are included for future use.
  */
final case class RawIngestConfig(
  mapping: Map[String, String],
  idColumn: String = "record_id",
  tableFieldName: String = "table"
) {
  // aliases to be tolerant with different field names used elsewhere
  val specs: Map[String, String]  = mapping
  val fields: Map[String, String] = mapping

  def get(key: String): Option[String] = mapping.get(key)
  def contains(key: String): Boolean   = mapping.contains(key)
}
