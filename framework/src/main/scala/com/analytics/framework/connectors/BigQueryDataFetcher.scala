package com.analytics.framework.connectors

import org.apache.beam.sdk.transforms.DoFn
import com.google.cloud.bigquery._
import com.analytics.framework.pipeline.core.NotificationEvent
import scala.jdk.CollectionConverters._

case class FetchedData(recordId: String, row: java.util.Map[String, AnyRef])

class BigQueryDataFetcher(projectId: String, dataset: String, table: String, idColumn: String)
  extends DoFn[NotificationEvent, FetchedData] {

  @transient private var bq: BigQuery = _

  @org.apache.beam.sdk.transforms.DoFn.Setup
  def setup(): Unit = {
    bq = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService
  }

  @org.apache.beam.sdk.transforms.DoFn.ProcessElement
  def process(ctx: DoFn[NotificationEvent, FetchedData]#ProcessContext): Unit = {
    val e = ctx.element()
    val ids = e.recordIds.asScala.map(id => s"'$id'").mkString(",")
    val sql = s"""SELECT * FROM `%s.%s.%s` WHERE %s IN (%s)""".format(projectId, dataset, table, idColumn, ids)

    val config = QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build()
    val result = bq.query(config)

    val schema = result.getSchema
    for (row <- result.iterateAll().asScala) {
      val map = new java.util.HashMap[String, AnyRef]()
      var idx = 0
      for (field <- schema.getFields.asScala) {
        val v = row.get(idx)
        map.put(field.getName, if (v == null || v.isNull) null else v.getValue)
        idx += 1
      }
      val ridField = Option(map.get(idColumn)).map(_.toString).getOrElse(java.util.UUID.randomUUID().toString)
      ctx.output(FetchedData(ridField, map))
    }
  }
}