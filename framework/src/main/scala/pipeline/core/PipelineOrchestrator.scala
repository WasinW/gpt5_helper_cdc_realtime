package com.analytics.framework.pipeline.core

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.windowing.{FixedWindows, Window}
import org.joda.time.Duration

import com.analytics.framework.utils.ConfigLoader
import com.analytics.framework.pipeline.ingestion._
import com.analytics.framework.pipeline.transformation._
import com.analytics.framework.modules.dependency.DependencyChecker
import com.analytics.framework.modules.reconciliation.ReconciliationEngine
import com.analytics.framework.modules.quality.DataQualityEngine
import com.analytics.framework.modules.audit.AuditLogger
import com.analytics.framework.connectors._

/**
  * The [[PipelineOrchestrator]] is responsible for wiring together all
  * stages of the CDC pipeline.  It reads configuration for a given domain
  * and table, instantiates the appropriate ingestion strategy and
  * downstream transformations, and builds an Apache Beam pipeline
  * configured for Google Cloud Dataflow.
  *
  * @param projectId  GCP project identifier
  * @param domain     Name of the business domain (e.g. "member")
  * @param configPath Path to the domain configuration (YAML)
  */
class PipelineOrchestrator(
    projectId: String,
    domain: String,
    configPath: String
) {

  // Load domain‑specific configuration.  The ConfigLoader reads YAML
  // into a generic map.  See utils/ConfigLoader.scala for details.
  private val config = ConfigLoader.loadDomainConfig(configPath)

  /**
    * Create Dataflow pipeline options with reasonable defaults for
    * streaming execution.  Adjust the number of workers, machine
    * types and experiments according to your throughput and latency
    * requirements.
    */
  def createPipelineOptions(): PipelineOptions = {
    val options = PipelineOptionsFactory.create()
    options.as(classOf[StreamingOptions]).setStreaming(true)
    options.setJobName(s"${domain}-cdc-pipeline")
    // Set Dataflow specific parameters via reflection to avoid direct
    // dependency on google specific options classes
    options.as(classOf[java.util.Map[String, String]]).put("project", projectId)
    options.as(classOf[java.util.Map[String, String]]).put("region", "us-central1")
    // Additional runner experiments for performance
    options.as(classOf[java.util.Map[String, java.util.List[String]]])
      .put("experiments", java.util.Arrays.asList(
        "use_runner_v2",
        "enable_streaming_engine_resource_based_billing",
        "enable_batch_mode_in_streaming",
        "shuffle_mode=service"
      ))
    options
  }

  /**
    * Build the Beam pipeline.  This method demonstrates how to
    * construct the core stages and wire up optional modules like
    * reconciliation and data quality.  It intentionally leaves many
    * details unimplemented so that they can be customised per domain.
    */
  def buildPipeline(): Pipeline = {
    val pipeline = Pipeline.create(createPipelineOptions())

    // Set up connectors
    val pubsub = new PubSubConnector(projectId)
    val bq     = new BigQueryConnector(projectId)
    val s3     = new S3Connector()
    val gcs    = new GCSConnector(projectId)

    // Example ingestion: pick ingestion type based on config
    val ingestionType = config.get("ingestion_type").map(_.toString).getOrElse("incremental").toLowerCase
    val ingestion: IngestionStrategy = ingestionType match {
      case "full" | "fulldump"    => new FullDumpIngestion(pubsub, bq)
      case "incremental"           => new IncrementalIngestion(pubsub, bq)
      case "upsert"                => new UpsertIngestion(pubsub, bq)
      case "transform_structure"   => new TransformStructureIngestion(pubsub, bq)
      case other                    => throw new IllegalArgumentException(s"Unsupported ingestion type: $other")
    }

    // In a real implementation the following transforms would be Beam
    // transforms applied to PCollections.  Here we simply demonstrate
    // how one might call into the ingestion strategy and follow on
    // processing modules.  These calls should be replaced with
    // `PCollection.apply(...)` calls in a proper Beam pipeline.

    // Stage: consume notifications and fetch raw records
    val rawRecords = ingestion.read() // returns PCollection[Record] in real code

    // Stage: dependency check
    val dependencyChecker = new DependencyChecker()
    dependencyChecker.check(rawRecords)

    // Stage: reconciliation against AWS S3 (if enabled)
    if (config.get("reconcile_enabled").contains(true)) {
      val reconciliationEngine = new ReconciliationEngine(new SchemaMapper(), new ReconciliationAuditor())
      reconciliationEngine.reconcile(rawRecords, s3)
    }

    // Stage: data quality
    val dqEngine = new DataQualityEngine(new QualityRules(), new QualityAuditor())
    dqEngine.check(rawRecords)

    // Stage: transformations (refined and analytics)
    val refinedTransform   = new RefinedTransformation()
    val analyticsTransform = new AnalyticsTransformation()
    val refined   = refinedTransform.transform(rawRecords)
    val analytics = analyticsTransform.transform(refined)

    // Stage: write outputs
    bq.write(refined, table = s"${domain}_refined")
    bq.write(analytics, table = s"${domain}_analytics")

    // Stage: audit logging
    val audit = new AuditLogger(new AuditStorage(gcs))
    audit.logPipelineCompletion(domain, ingestionType)

    pipeline
  }
}