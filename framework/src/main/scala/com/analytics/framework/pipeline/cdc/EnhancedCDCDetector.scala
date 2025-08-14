package com.analytics.framework.pipeline.cdc

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import com.analytics.framework.core.models._
import scala.collection.mutable
import java.time.Instant

class EnhancedCDCDetector extends DoFn[FetchedData, CDCRecord] {
  
  private val recentCache = mutable.LinkedHashMap[String, CDCState]()
  private val MAX_CACHE_SIZE = 100000
  
  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    val element = c.element()
    val cacheKey = s"${element.tableName}:${element.recordId}"
    
    val operation = detectOperation(element)
    
    val cdcRecord = CDCRecord(
      recordId = element.recordId,
      tableName = element.tableName,
      operation = operation,
      beforeData = getBeforeData(cacheKey),
      afterData = element.data,
      changeTimestamp = System.currentTimeMillis(),
      metadata = element.metadata
    )
    
    updateCache(cacheKey, cdcRecord)
    c.output(cdcRecord)
  }
  
  private def detectOperation(data: FetchedData): CDCOperation = {
    val eventType = data.eventType.toLowerCase
    val exists = recentCache.contains(s"${data.tableName}:${data.recordId}")
    
    (eventType, exists) match {
      case ("delete", _) => CDCOperation.Delete
      case ("insert" | "create", false) => CDCOperation.Insert
      case ("update", _) => CDCOperation.Update
      case _ => CDCOperation.Unknown
    }
  }
  
  private def getBeforeData(key: String): Option[Map[String, Any]] = {
    recentCache.get(key).map(_.data)
  }
  
  private def updateCache(key: String, record: CDCRecord): Unit = {
    if (recentCache.size >= MAX_CACHE_SIZE) {
      recentCache.remove(recentCache.head._1)
    }
    recentCache.put(key, CDCState(
      recordId = record.recordId,
      data = record.afterData,
      operation = record.operation,
      timestamp = record.changeTimestamp
    ))
  }
}

case class CDCState(
  recordId: String,
  data: Map[String, Any],
  operation: CDCOperation,
  timestamp: Long
)

sealed trait CDCOperation
object CDCOperation {
  case object Insert extends CDCOperation
  case object Update extends CDCOperation
  case object Delete extends CDCOperation
  case object Unknown extends CDCOperation
}

case class FetchedData(
  recordId: String,
  tableName: String,
  eventType: String,
  data: Map[String, Any],
  metadata: Map[String, String] = Map.empty
)

case class CDCRecord(
  recordId: String,
  tableName: String,
  operation: CDCOperation,
  beforeData: Option[Map[String, Any]],
  afterData: Map[String, Any],
  changeTimestamp: Long,
  metadata: Map[String, String] = Map.empty
)
