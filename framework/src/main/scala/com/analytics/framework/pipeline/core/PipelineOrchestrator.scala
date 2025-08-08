package com.analytics.framework.pipeline.core

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.{ParDo}
import org.apache.beam.sdk.io.gcp.pubsub.{PubsubIO, PubsubMessage}
import org.apache.beam.sdk.transforms.windowing._
import org.joda.time.Duration

import com.analytics.framework.utils.ConfigLoader
import com.analytics.framework.connectors.BigQueryDataFetcher
import com.analytics.framework.modules.reconciliation.{ReconciliationEngine, SchemaMapper}
import com.analytics.framework.modules.quality.{DataQualityEngine, DQRule}
import com.analytics.framework.modules.audit.AuditLogger
import com.analytics.framework.transform.{StructureTransformer, RefinedTransformation, AnalyticsTransformation}
import scala.jdk.CollectionConverters._

class PipelineOrchestrator(
  projectId: String,
  domain: String,
  table: String,
  configPath: String
) {

  private val cfg = ConfigLoader.load(configPath)
  private val jobName = s"${domain}-${table}-cdc-full"

  def build(): Pipeline = {
    val opts = PipelineOptionsFactory.create()
    val df = opts.as(classOf[org.apache.beam.runners.dataflow.options.DataflowPipelineOptions])
    df.setProject(projectId)
    df.setRegion("asia-southeast1")
    df.setJobName(jobName)
    df.setStreaming(true)
    df.setEnableStreamingEngine(true)
    val p = Pipeline.create(opts)

    val rawDataset = cfg.getOrDefault("raw_dataset", s"${domain}_raw").toString
    val structureDataset = cfg.getOrDefault("structure_dataset", s"${domain}_structure").toString
    val refinedDataset = cfg.getOrDefault("refined_dataset", s"${domain}_refined").toString
    val analyticsDataset = cfg.getOrDefault("analytics_dataset", s"${domain}_analytics").toString
    val frameworkDataset = cfg.getOrDefault("framework_dataset", s"${domain}_framework").toString
    val idColumn = cfg.getOrDefault("id_column", "record_id").toString

    val notifications = p
      .apply("ReadNotifications",
        PubsubIO.readMessagesWithAttributes()
          .fromSubscription(s"projects/$projectId/subscriptions/${domain}-notification-sub")
          .withIdAttribute("message_id")
          .withTimestampAttribute("event_timestamp"))
      .apply("Window1s",
        Window.into[PubsubMessage](FixedWindows.of(Duration.standardSeconds(1)))
          .triggering(
            AfterWatermark.pastEndOfWindow()
              .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMillis(500)))
              .withLateFirings(AfterPane.elementCountAtLeast(1))
          ).withAllowedLateness(Duration.standardMinutes(5))
          .discardingFiredPanes()
      )
      .apply("ProcessNotifications", ParDo.of(new NotificationProcessor(table)))

    val fetched = notifications.apply("FetchFromBQ",
      ParDo.of(new BigQueryDataFetcher(projectId, rawDataset, table, idColumn)))

    val cdc = fetched.apply("CDCDetect", ParDo.of(new CDCDetector()))

    // RAW
    val raw = cdc
      .apply("InsertRaw", ParDo.of(new RawInserter(projectId, rawDataset, table)))
      .apply("AuditRaw", ParDo.of(new AuditLogger(projectId, frameworkDataset, table, "raw")))

    val reconConf = cfg.get("reconciler").asInstanceOf[java.util.Map[String, Object]]
    val awsBucket = reconConf.get("aws_bucket").toString
    def prefix(z:String) = reconConf.getOrDefault(s"aws_prefix_${z}", s"path/to/${domain}/${z}/").toString

    val mapping = Option(reconConf.get("mapping_schemas"))
      .map(_.asInstanceOf[java.util.Map[String, java.util.Map[String, String]]]
      .asScala.view.mapValues(_.asScala.toMap).toMap).getOrElse(Map.empty[String, Map[String, String]])

    val rawRecon = raw.apply("ReconcileRaw",
      ParDo.of(new ReconciliationEngine(awsBucket, prefix("raw"), idColumn, mapping)))

    // DQ
    val dqConf = cfg.get("dq").asInstanceOf[java.util.Map[String, Object]]
    val notNullCols = Option(dqConf.get("not_null")).map(_.asInstanceOf[java.util.List[String]].asScala.toList).getOrElse(Nil)
    val rules = (if (notNullCols.nonEmpty) List(DQRule("not_null", notNullCols)) else Nil)
    val rawDQ = rawRecon.apply("DQRaw", ParDo.of(new DataQualityEngine(rules)))
      .apply("AuditRawDQ", ParDo.of(new AuditLogger(projectId, frameworkDataset, table, "raw_dq")))

    // STRUCTURE
    val structured = rawDQ.apply("StructureTransform", ParDo.of(new StructureTransformer(Map.empty)))
      .apply("AuditStructure", ParDo.of(new AuditLogger(projectId, frameworkDataset, table, "structure")))

    val structureRecon = structured.apply("ReconcileStructure",
      ParDo.of(new ReconciliationEngine(awsBucket, prefix("structure"), idColumn, mapping)))
    val structureDQ = structureRecon.apply("DQStructure", ParDo.of(new DataQualityEngine(rules)))
      .apply("AuditStructureDQ", ParDo.of(new AuditLogger(projectId, frameworkDataset, table, "structure_dq")))

    // REFINED
    val refined = structureDQ.apply("RefinedTransform", ParDo.of(new RefinedTransformation()))
      .apply("AuditRefined", ParDo.of(new AuditLogger(projectId, frameworkDataset, table, "refined")))

    val refinedRecon = refined.apply("ReconcileRefined",
      ParDo.of(new ReconciliationEngine(awsBucket, prefix("refined"), idColumn, mapping)))
    val refinedDQ = refinedRecon.apply("DQRefined", ParDo.of(new DataQualityEngine(rules)))
      .apply("AuditRefinedDQ", ParDo.of(new AuditLogger(projectId, frameworkDataset, table, "refined_dq")))

    // ANALYTICS
    val analytics = refinedDQ.apply("AnalyticsTransform", ParDo.of(new AnalyticsTransformation()))
      .apply("AuditAnalytics", ParDo.of(new AuditLogger(projectId, frameworkDataset, table, "analytics")))

    val analyticsRecon = analytics.apply("ReconcileAnalytics",
      ParDo.of(new ReconciliationEngine(awsBucket, prefix("analytics"), idColumn, mapping)))
    val analyticsDQ = analyticsRecon.apply("DQAnalytics", ParDo.of(new DataQualityEngine(rules)))
      .apply("AuditAnalyticsDQ", ParDo.of(new AuditLogger(projectId, frameworkDataset, table, "analytics_dq")))

    p
  }
}