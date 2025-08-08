package com.analytics.member.dataflow

import com.analytics.framework.pipeline.core.PipelineOrchestrator

/**
  * Entry point for the Member CDC pipeline.  This program can be
  * packaged into a Dataflow Flex template or run directly with
  * `sbt run` (in direct runner mode).  It loads configuration from
  * the `config` directory and delegates the heavy lifting to the
  * framework orchestrator.
  */
object MemberPipeline {
  def main(args: Array[String]): Unit = {
    // Expect a single argument: path to the pipeline configuration
    val configPath = args.headOption.getOrElse("config/pipeline_config.yaml")
    val projectId  = sys.env.getOrElse("GCP_PROJECT", "your-gcp-project")
    val orchestrator = new PipelineOrchestrator(projectId, "member", configPath)
    val pipeline = orchestrator.buildPipeline()
    // In a real application you would call pipeline.run().waitUntilFinish()
    println("MemberPipeline: pipeline constructed.  Submit to Dataflow with run().")
  }
}