package com.analytics.framework.pipeline.core

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.{PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.{ParDo}
import org.apache.beam.sdk.io.gcp.pubsub.{PubsubIO, PubsubMessage}
import org.apache.beam.sdk.transforms.windowing._
import org.joda.time.Duration

import com.analytics.framework.utils.ConfigLoader
import com.analytics.framework.connectors.BigQueryDataFetcher

class PipelineOrchestrator(
  projectId: String,
  domain: String,
  table: String,
  configPath: String
) {

  private val cfg = ConfigLoader.load(configPath)
  private val jobName = s"${domain}-${table}-cdc-raw"

  def build(): Pipeline = {
    val options = PipelineOptionsFactory.create()
    options.setJobName(jobName)
    options.as(classOf[org.apache.beam.runners.dataflow.options.DataflowPipelineOptions]).setProject(projectId)
    options.as(classOf[org.apache.beam.runners.dataflow.options.DataflowPipelineOptions]).setRegion("europe-west3")
    options.as(classOf[org.apache.beam.runners.dataflow.options.DataflowPipelineOptions]).setStreaming(true)
    options.as(classOf[org.apache.beam.runners.dataflow.options.DataflowPipelineOptions]).setEnableStreamingEngine(true)

    val p = Pipeline.create(options)

    val notifications = p
      .apply("ReadNotifications",
        PubsubIO.readMessagesWithAttributes()
          .fromSubscription(s"projects/$projectId/subscriptions/${domain}-notification-sub")
          .withIdAttribute("message_id")
          .withTimestampAttribute("event_timestamp")
      )
      .apply("Window1s",
        Window.into[PubsubMessage](FixedWindows.of(Duration.standardSeconds(1)))
          .triggering(
            AfterWatermark.pastEndOfWindow()
              .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMillis(500)))
              .withLateFirings(AfterPane.elementCountAtLeast(1))
          )
          .withAllowedLateness(Duration.standardMinutes(5))
          .discardingFiredPanes()
      )
      .apply("ProcessNotifications", ParDo.of(new NotificationProcessor(cfg, table)))

    val fetched = notifications.apply("FetchFromBigQuery",
      ParDo.of(new BigQueryDataFetcher(projectId, domain, table))
    )

    val cdc = fetched.apply("CDCDetect", ParDo.of(new CDCDetector()))

    val inserted = cdc.apply("InsertRaw", ParDo.of(new RawInserter(projectId, domain, table)))

    val reconciled = inserted.apply("ReconcileRaw", ParDo.of(new ReconciliationEngine()))

    val dq = reconciled.apply("DQRaw", ParDo.of(new DataQualityEngine()))

    dq.apply("AuditRaw", ParDo.of(new AuditLogger(projectId, domain, table, "raw")))

    p
  }
}