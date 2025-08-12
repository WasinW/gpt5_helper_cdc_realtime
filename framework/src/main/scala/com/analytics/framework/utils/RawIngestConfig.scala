package com.analytics.framework.utils

/**
 * Configuration for raw ingestion mappings.
 *
 * A `RawIngestConfig` defines how to extract fields from an incoming message
 * when converting it into a raw record.  Each entry in the `fields` list
 * specifies a target column name and a specification describing how to
 * obtain the value from the message.  Specifications are interpreted by
 * `MessageToRawStage` and may begin with "attr:" to read from the
 * Pub/Sub message attributes or "json:" to read a dot‑path from the
 * JSON payload.
 *
 * @param idField optional name of the message attribute or JSON field
 *                that uniquely identifies the record
 * @param tableField optional name of the message attribute or JSON field
 *                   that contains the destination table name
 * @param fields list of (targetColumn, specification) pairs
 */
final case class RawIngestConfig(
    idField: Option[String] = None,
    tableField: Option[String] = None,
    fields: List[(String, String)] = List.empty
)
