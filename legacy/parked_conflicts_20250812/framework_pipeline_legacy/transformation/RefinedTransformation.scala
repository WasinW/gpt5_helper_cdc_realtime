package com.analytics.framework.pipeline.transformation

/**
  * Generic transformation for refined zone.  Implement your business
  * logic here to derive refined data from raw/structure zones.  The
  * transformation should be idempotent and stateless – all stateful
  * operations should reside in ingestion strategies or dedicated
  * modules.
  */
class RefinedTransformation {
  /**
    * Apply the refined transformation.  Replace the return type with
    * your PCollection of refined records when integrating with
    * Apache Beam.
    */
  def transform(rawRecords: Any): Any = {
    println("RefinedTransformation: applying business logic for refined zone")
    rawRecords // pass through in this stub
  }
}