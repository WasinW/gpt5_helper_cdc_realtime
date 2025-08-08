package com.analytics.framework.pipeline.ingestion

import com.analytics.framework.connectors.{PubSubConnector, BigQueryConnector}

/**
  * Ingestion strategy for upsert semantics.  Similar to incremental
  * ingestion but records are written downstream in an idempotent
  * manner.  Deletes are typically represented as tombstone updates
  * rather than physical removals.
  */
class UpsertIngestion(pubsub: PubSubConnector, bq: BigQueryConnector) extends IngestionStrategy {
  override def read(): Any = {
    println("UpsertIngestion: consuming messages and performing upserts")
    new Object()
  }
}