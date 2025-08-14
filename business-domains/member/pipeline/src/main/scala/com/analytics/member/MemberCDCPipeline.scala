// member-pipeline/src/main/scala/com/analytics/member/MemberCDCPipeline.scala
package com.analytics.member

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import com.analytics.framework.pipeline.core.WindowingStrategy._
import com.analytics.framework.connectors.BqStorageWriteConnector

class MemberCDCPipeline(config: PipelineConfig) {
  
  def run(): Unit = {
    val pipeline = Pipeline.create(config.options)
    
    // ============================================
    // Window 1: Notification Processing (1 second)
    // ============================================
    val notifications = pipeline
      .apply("ReadFromPubSub", 
        PubsubIO.readMessagesWithAttributes()
          .fromSubscription(config.subscription)
      )
      .apply("Window1_Notification", 
        applyWindow(_, NotificationWindow)
      )
      .apply("ProcessNotifications", 
        ParDo.of(new NotificationProcessor())
      )
    
    // ============================================
    // Window 2: Raw Zone (60 seconds)
    // ============================================
    val rawRecords = notifications
      .apply("FetchFromAPI", 
        ParDo.of(new MemberApiProcessor(config.api))
      )
      .apply("Window2_Raw", 
        applyWindow(_, RawWindow)
      )
      .apply("CDCDetection", 
        ParDo.of(new EnhancedCDCDetector())
      )
      .apply("WriteToRaw_13Tables", 
        ParDo.of(new RawZoneWriter(config.rawTables))
      )
    
    // Parallel Reconciliation for Raw
    rawRecords.apply("ReconcileRaw", 
      ParDo.of(new S3ReconciliationEngine(config.s3, config.schemaMapping))
    )
    
    // ============================================
    // Window 3: Structure Zone (60 seconds)
    // ============================================
    val structureRecords = rawRecords
      .apply("Window3_Structure", 
        applyWindow(_, StructureWindow)
      )
      .apply("TypeConversion", 
        ParDo.of(new StructureTransformer())
      )
      .apply("WriteToStructure_13Tables",
        new BqStorageWriteConnector(
          config.projectId,
          config.datasets.structure,
          "_"
        ).writeDynamic(_)
      )
    
    // ============================================
    // Window 4: Refined Zone (300 seconds)
    // ============================================
    val refinedRecords = structureRecords
      .apply("Window4_Refined", 
        applyWindow(_, RefinedWindow)
      )
      .apply("BusinessLogicTransform", 
        ParDo.of(new MemberRefinedTransformer())
      )
      .apply("WriteToRefined_7Tables",
        new BqStorageWriteConnector(
          config.projectId,
          config.datasets.refined,
          "_"
        ).writeDynamic(_)
      )
    
    // ============================================
    // Window 5: Analytics Zone (300 seconds)
    // ============================================
    val analyticsRecords = refinedRecords
      .apply("Window5_Analytics", 
        applyWindow(_, AnalyticsWindow)
      )
      .apply("AggregateToMsMember", 
        ParDo.of(new MemberAnalyticsTransformer())
      )
      .apply("WriteToAnalytics_MsMember",
        new BqStorageWriteConnector(
          config.projectId,
          config.datasets.analytics,
          "ms_member"
        ).write(_, config.schemas.msMember, identity)
      )
    
    // Run pipeline
    val result = pipeline.run()
    if (config.waitUntilFinish) {
      result.waitUntilFinish()
    }
  }
}