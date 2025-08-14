// framework/src/main/scala/com/analytics/framework/pipeline/cdc/CDCDetector.scala
package com.analytics.framework.pipeline.cdc

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.state.{StateSpecs, ValueState}
import org.apache.beam.sdk.values.KV
import com.analytics.framework.core.models._
import scala.collection.mutable

class EnhancedCDCDetector extends DoFn[FetchedData, CDCRecord] {
  
  // State for deduplication
  @StateId("seen") 
  private val seenState = StateSpecs.value[String]()
  
  // In-memory cache for performance
  private val recentCache = mutable.LinkedHashMap[String, CDCState]()
  private val MAX_CACHE_SIZE = 100000
  
  @ProcessElement
  def processElement(
    @Element element: FetchedData,
    @StateId("seen") seen: ValueState[String],
    out: OutputReceiver[CDCRecord]
  ): Unit = {
    
    val cacheKey = s"${element.tableName}:${element.recordId}"
    
    // Check for duplicate
    val previousHash = seen.read()
    val currentHash = hashRecord(element)
    
    if (previousHash != currentHash) {
      // Detect CDC operation
      val operation = detectOperation(element)
      
      val cdcRecord = CDCRecord(
        recordId = element.recordId,
        tableName = element.tableName,
        operation = operation,
        beforeData = getBeforeData(cacheKey),
        afterData = element.data,
        changeTimestamp = System.currentTimeMillis(),
        metadata = element.metadata ++ Map(
          "cdc_hash" -> currentHash,
          "cdc_detected_at" -> Instant.now().toString
        )
      )
      
      // Update state and cache
      seen.write(currentHash)
      updateCache(cacheKey, cdcRecord)
      
      out.output(cdcRecord)
    }
  }
  
  private def detectOperation(data: FetchedData): CDCOperation = {
    val eventType = data.eventType.toLowerCase
    val existingData = recentCache.get(s"${data.tableName}:${data.recordId}")
    
    (eventType, existingData.isDefined) match {
      case ("delete", _) => CDCOperation.Delete
      case ("insert" | "create", false) => CDCOperation.Insert
      case ("insert" | "create", true) => CDCOperation.Upsert
      case ("update", _) => CDCOperation.Update
      case _ => CDCOperation.Unknown
    }
  }
  
  private def updateCache(key: String, record: CDCRecord): Unit = {
    // LRU eviction
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
  
  private def hashRecord(data: FetchedData): String = {
    import java.security.MessageDigest
    val content = data.data.toSeq.sortBy(_._1).map(_.toString).mkString("|")
    MessageDigest.getInstance("MD5")
      .digest(content.getBytes)
      .map("%02x".format(_))
      .mkString
  }
}