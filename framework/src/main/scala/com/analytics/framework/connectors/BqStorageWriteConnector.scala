// framework/src/main/scala/com/analytics/framework/connectors/BqStorageWriteConnector.scala
package com.analytics.framework.connectors

import org.apache.beam.sdk.io.gcp.bigquery._
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.PCollection
import com.google.api.services.bigquery.model.{TableRow, TableSchema}
import com.analytics.framework.core.models._

class BqStorageWriteConnector(
  projectId: String,
  dataset: String,
  table: String
) {
  
  def write[T](
    input: PCollection[T],
    schema: TableSchema,
    transformer: T => TableRow
  ): Unit = {
    
    input
      .apply("ConvertToTableRow", 
        ParDo.of(new DoFn[T, TableRow]() {
          @ProcessElement
          def process(c: ProcessContext): Unit = {
            c.output(transformer(c.element()))
          }
        })
      )
      .apply(s"WriteToBQ_${table}",
        BigQueryIO.writeTableRows()
          .to(s"$projectId:$dataset.$table")
          .withSchema(schema)
          .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(WriteDisposition.WRITE_APPEND)
          
          // ใช้ Storage Write API
          .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
          
          // Configuration for optimal performance
          .withTriggeringFrequency(org.joda.time.Duration.standardSeconds(60))
          .withNumStorageWriteApiStreams(50)
          .withStorageWriteApiAtLeastOnce(false) // Exactly-once
          
          // Auto-sharding for better parallelism
          .withAutoSharding()
          
          // Failed inserts handling
          .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
          .withExtendedErrorInfo()
      )
  }
}