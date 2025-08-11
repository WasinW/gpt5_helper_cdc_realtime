package com.analytics.framework.pipeline.ingestion

/**
  * Base trait for ingestion strategies.  Each implementation must
  * provide a [[read]] method that returns a stream or collection
  * of raw records from the upstream systems.
  */
trait IngestionStrategy {
  /**
    * Begin reading from the source and return a collection of raw
    * records.  In a Beam context this would return a
    * `PCollection[Record]`.  This trait returns `Any` to avoid
    * bringing the full Beam API into the interface – update as
    * needed for your use case.
    */
  def read(): Any
}