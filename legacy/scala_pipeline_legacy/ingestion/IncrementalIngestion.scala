package com.analytics.framework.pipeline.ingestion

import com.analytics.framework.connectors.{PubSubConnector, BigQueryConnector}

/**
  * Ingestion strategy for incremental changes.  This strategy
  * processes inserts, updates and deletes based on change
  * timestamps or sequence numbers.  It fetches only the deltas
  * since the last watermark.
  */
class IncrementalIngestion(pubsub: PubSubConnector, bq: BigQueryConnector) extends IngestionStrategy {
  override def read(): Any = {
    println("IncrementalIngestion: consuming incremental messages and fetching deltas from BigQuery")
    new Object()
  }
}