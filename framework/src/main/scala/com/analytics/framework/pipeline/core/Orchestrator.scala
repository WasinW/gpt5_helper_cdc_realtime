package com.analytics.framework.pipeline.core
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.{ParDo, Flatten, DoFn}
import org.apache.beam.sdk.io.gcp.pubsub.{PubsubIO, PubsubMessage}
import org.apache.beam.sdk.transforms.windowing._
import org.joda.time.Duration
import com.analytics.framework.pipeline.io.NotificationParser
import com.analytics.framework.pipeline.process.MemberProfileApiFetcher
import com.analytics.framework.pipeline.model.{CDCRecord, Notification}
import com.analytics.framework.utils.ConfigLoader
import com.analytics.framework.modules.reconcile.{ReconcileDoFn, ReconcileLog}
import com.analytics.framework.modules.dq.{DataQualityDoFn, NotNull}
import org.apache.beam.sdk.io.TextIO
class Orchestrator(projectId: String, configPath: String) {
  def build(): Pipeline = {
    val cfg = ConfigLoader.load(configPath)
    val p = Pipeline.create(PipelineOptionsFactory.create())
    val domain = cfg.getOrDefault("domain", "member").toString
    val logs = cfg.get("logs").asInstanceOf[java.util.Map[String,Object]]
    val auditBucket = logs.get("audit_bucket").toString
    val pubsub = cfg.get("pubsub").asInstanceOf[java.util.Map[String,Object]]
    val subCreate = pubsub.getOrDefault("subscription_create", "member-events-create-sub").toString
    val subUpdate = pubsub.getOrDefault("subscription_update", "member-events-update-sub").toString
    val api = cfg.get("api").asInstanceOf[java.util.Map[String,Object]]
    val baseUrl = api.get("base_url").toString; val secretId = api.get("token_secret_id").toString
    val timeout = api.getOrDefault("timeout_sec", Int.box(10)).asInstanceOf[Int]; val rps = api.getOrDefault("rate_limit_rps", Int.box(30)).asInstanceOf[Int]
    val create = p.apply("ReadCreate", PubsubIO.readMessagesWithAttributes().fromSubscription(s"projects/$projectId/subscriptions/$subCreate"))
    val update = p.apply("ReadUpdate", PubsubIO.readMessagesWithAttributes().fromSubscription(s"projects/$projectId/subscriptions/$subUpdate"))
    def parse(stream: org.apache.beam.sdk.values.PCollection[PubsubMessage]) =
      stream.apply("Parse", ParDo.of(new NotificationParser()))
        .apply("Window1m", org.apache.beam.sdk.transforms.windowing.Window.into[Notification](FixedWindows.of(Duration.standardMinutes(1)))
          .triggering(AfterWatermark.pastEndOfWindow().withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(30))))
          .withAllowedLateness(Duration.standardMinutes(10)).discardingFiredPanes())
    val parsed = org.apache.beam.sdk.values.PCollectionList.of(parse(create)).and(parse(update)).apply("Merge", Flatten.pCollections())
    val fetched = parsed.apply("FetchMemberAPI", ParDo.of(new MemberProfileApiFetcher(projectId, baseUrl, secretId, timeout, rps)))
    val recon = cfg.get("reconciler").asInstanceOf[java.util.Map[String,Object]]
    val awsBucket = recon.get("aws_bucket").toString; val rawPrefix = recon.get("prefix_raw").toString
    val reconLogs = fetched.apply("ReconcileRaw", ParDo.of(new ReconcileDoFn(awsBucket, rawPrefix, "accountId")))
    val reconPath = s"$auditBucket/reconcile_log/$domain/raw"
    reconLogs.apply("ReconToJson", ParDo.of(new DoFn[ReconcileLog, String](){ @DoFn.ProcessElement def p(ctx: DoFn[ReconcileLog, String]#ProcessContext): Unit = { val r = ctx.element(); val json = "{{\"recordId\":\""+r.recordId+"\",\"table\":\""+r.table+"\",\"zone\":\""+r.zone+"\",\"gcpCount\":"+r.gcpCount+",\"awsCount\":"+r.awsCount+",\"countMatch\":"+r.countMatch+",\"ts\":"+r.ts+"}}"; ctx.output(json) }}))
      .apply("WriteRecon", TextIO.write().to(reconPath).withWindowedWrites().withNumShards(1).withSuffix(".jsonl"))
    val dq = fetched.apply("DQ", ParDo.of(new DataQualityDoFn(List(NotNull(List("accountId"))))))
    val dqPath = s"$auditBucket/data_quality_log/$domain/raw"
    dq.apply("DQToJson", ParDo.of(new DoFn[(CDCRecord, com.analytics.framework.modules.dq.DQResult), String](){ @DoFn.ProcessElement def p(ctx: DoFn[(CDCRecord, com.analytics.framework.modules.dq.DQResult), String]#ProcessContext): Unit = { val t = ctx.element(); val passed = t._2.passed; val json = "{{\"recordId\":\""+t._1.recordId+"\",\"passed\":"+passed+",\"detail\":\""+t._2.detail.replace("\\","\\\\").replace("\"","\\\"")+"\",\"zone\":\"raw\"}}"; ctx.output(json) }}))
      .apply("WriteDQ", TextIO.write().to(dqPath).withWindowedWrites().withNumShards(1).withSuffix(".jsonl"))
    val pipePath = s"$auditBucket/pipeline_log/$domain/raw"
    parsed.apply("PipelineLog", ParDo.of(new DoFn[Notification, String](){ @DoFn.ProcessElement def p(ctx: DoFn[Notification, String]#ProcessContext): Unit = { val n = ctx.element(); ctx.output("{{\"messageId\":\""+n.messageId+"\",\"eventType\":\""+n.eventType+"\",\"ts\":\""+n.ts+"\"}}") }}))
      .apply("WritePipe", TextIO.write().to(pipePath).withWindowedWrites().withNumShards(1).withSuffix(".jsonl"))
    p
  }
}
