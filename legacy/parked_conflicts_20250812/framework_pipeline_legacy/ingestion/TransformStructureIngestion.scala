package com.analytics.framework.pipeline.ingestion

import com.analytics.framework.connectors.{PubSubConnector, BigQueryConnector}

/**
  * Ingestion strategy for transforming raw records into a structured
  * representation.  This is typically used for data type conversions
  * and minor enrichments immediately after ingestion.
  */
class TransformStructureIngestion(pubsub: PubSubConnector, bq: BigQueryConnector) extends IngestionStrategy {
  override def read(): Any = {
    println("TransformStructureIngestion: consuming messages and performing structural transforms")
    new Object()
  }
}