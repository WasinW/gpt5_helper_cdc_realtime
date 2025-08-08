package com.analytics.framework.pipeline.ingestion

import com.analytics.framework.connectors.{PubSubConnector, BigQueryConnector}

/**
  * Ingestion strategy for full table dumps.  This strategy ignores
  * incremental change markers and simply reimports the entire data
  * set whenever a notification arrives.  In a real implementation
  * this would likely batch up records and emit them on a schedule.
  *
  * @param pubsub Connector for receiving notifications
  * @param bq     Connector for fetching raw data from BigQuery
  */
class FullDumpIngestion(pubsub: PubSubConnector, bq: BigQueryConnector) extends IngestionStrategy {
  override def read(): Any = {
    // Read messages from Pub/Sub and fetch the complete table from BigQuery
    // In Beam this would be a series of PTransforms.  Here we simply
    // return a placeholder object.
    println("FullDumpIngestion: reading full dump of table from BigQuery")
    new Object()
  }
}