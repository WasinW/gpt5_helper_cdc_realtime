// framework/src/main/scala/com/analytics/framework/modules/reconciliation/S3ReconciliationEngine.scala
package com.analytics.framework.modules.reconciliation

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import org.apache.beam.sdk.transforms.DoFn
import com.analytics.framework.core.models._
import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind.ObjectMapper

class S3ReconciliationEngine(
  s3Config: S3Config,
  schemaMapping: Map[String, Map[String, String]]
) extends DoFn[ProcessedRecord, ReconciliationResult] {
  
  @transient private var s3Client: S3Client = _
  @transient private var objectMapper: ObjectMapper = _
  
  @Setup
  def setup(): Unit = {
    s3Client = S3Client.builder()
      .region(Region.of(s3Config.region))
      .credentialsProvider(getCredentialsProvider())
      .build()
    objectMapper = new ObjectMapper()
  }
  
  @ProcessElement
  def processElement(
    @Element record: ProcessedRecord,
    @Timestamp timestamp: Instant,
    out: OutputReceiver[ReconciliationResult]
  ): Unit = {
    
    val s3Data = fetchS3Snapshot(record.tableName, record.zone, timestamp)
    val mappedS3Data = applySchemaMapping(s3Data, record.tableName)
    
    val comparison = compareData(
      gcpRecord = record,
      s3Records = mappedS3Data
    )
    
    val result = ReconciliationResult(
      recordId = record.recordId,
      tableName = record.tableName,
      zone = record.zone,
      gcpCount = 1,
      awsCount = mappedS3Data.size,
      matchedRows = comparison.matches,
      mismatchedRows = comparison.mismatches,
      missingInGcp = comparison.missingInGcp,
      missingInAws = comparison.missingInAws,
      status = if (comparison.isFullMatch) "MATCHED" else "MISMATCHED",
      details = comparison.details,
      windowStart = timestamp.minus(Duration.standardMinutes(5)).toString,
      windowEnd = timestamp.toString
    )
    
    out.output(result)
    
    // Send alerts for mismatches
    if (!comparison.isFullMatch && s3Config.alertOnMismatch) {
      sendMismatchAlert(result)
    }
  }
  
  private def fetchS3Snapshot(
    table: String,
    zone: String,
    timestamp: Instant
  ): List[Map[String, Any]] = {
    
    val prefix = s"${s3Config.basePrefix}/$zone/$table/dt=${timestamp.toString("yyyy-MM-dd")}/"
    
    val listRequest = ListObjectsV2Request.builder()
      .bucket(s3Config.bucket)
      .prefix(prefix)
      .build()
    
    val objects = s3Client.listObjectsV2(listRequest)
      .contents()
      .asScala
      .filter(_.key().endsWith(".jsonl"))
    
    objects.flatMap { obj =>
      val getRequest = GetObjectRequest.builder()
        .bucket(s3Config.bucket)
        .key(obj.key())
        .build()
      
      val response = s3Client.getObject(getRequest)
      val lines = scala.io.Source.fromInputStream(response).getLines()
      
      lines.map { line =>
        objectMapper.readValue(line, classOf[Map[String, Any]])
      }.toList
    }.toList
  }
  
  private def applySchemaMapping(
    s3Data: List[Map[String, Any]],
    table: String
  ): List[Map[String, Any]] = {
    val mapping = schemaMapping.getOrElse(table, Map.empty)
    
    s3Data.map { record =>
      record.map { case (s3Field, value) =>
        val gcpField = mapping.getOrElse(s3Field, s3Field)
        gcpField -> value
      }
    }
  }
  
  private def compareData(
    gcpRecord: ProcessedRecord,
    s3Records: List[Map[String, Any]]
  ): ComparisonResult = {
    
    val gcpKey = gcpRecord.recordId
    val s3Match = s3Records.find(_.get("record_id") == Some(gcpKey))
    
    s3Match match {
      case Some(s3Record) =>
        val differences = findDifferences(gcpRecord.data, s3Record)
        ComparisonResult(
          matches = if (differences.isEmpty) 1 else 0,
          mismatches = if (differences.nonEmpty) 1 else 0,
          missingInGcp = Nil,
          missingInAws = Nil,
          details = differences,
          isFullMatch = differences.isEmpty
        )
      case None =>
        ComparisonResult(
          matches = 0,
          mismatches = 0,
          missingInGcp = Nil,
          missingInAws = List(gcpKey),
          details = List(s"Record $gcpKey not found in S3"),
          isFullMatch = false
        )
    }
  }
}