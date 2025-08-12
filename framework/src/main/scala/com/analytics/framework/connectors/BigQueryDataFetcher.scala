package com.analytics.framework.connectors
import com.google.cloud.bigquery.{BigQuery,BigQueryOptions,QueryJobConfiguration,TableResult,FieldValueList,FieldList}
import scala.collection.JavaConverters._
object BigQueryDataFetcher{
  final case class Event(recordIds: java.util.List[String])
  def fetchByIds(projectId:String, dataset:String, table:String, idColumn:String, e: Event): List[Map[String,Any]] = {
    val bq: BigQuery = BigQueryOptions.getDefaultInstance.getService
    val ids = e.recordIds.asScala.map(id => s"'$id'").mkString(",")
    val query = s"SELECT * FROM `${projectId}.${dataset}.${table}` WHERE ${idColumn} IN (${ids})"
    val cfg = QueryJobConfiguration.newBuilder(query).build()
    val result: TableResult = bq.query(cfg)
    val fields = result.getSchema.getFields.asScala
    val out = scala.collection.mutable.ListBuffer.empty[Map[String,Any]]
    for (row: FieldValueList <- result.iterateAll().asScala) {
      val map = fields.map { field =>
        val v = row.get(field.getName)
        val any: Any =
          if (v.isNull) null
          else if (v.getValue.isInstanceOf[java.lang.Long]) v.getLongValue
          else v.getValue
        field.getName -> any
      }.toMap
      val ridField = scala.Option(map.get(idColumn)).map(_.toString).getOrElse(java.util.UUID.randomUUID().toString)
      out += map + ("_rid" -> ridField)
    }
    out.toList
  }
}
