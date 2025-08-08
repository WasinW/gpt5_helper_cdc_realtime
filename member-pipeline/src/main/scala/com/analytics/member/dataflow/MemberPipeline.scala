package com.analytics.member.dataflow

import com.analytics.framework.pipeline.core.PipelineOrchestrator

object MemberPipeline {
  def main(args: Array[String]): Unit = {
    val projectId = sys.env.getOrElse("GCP_PROJECT", "your-project-id")
    val domain = sys.props.getOrElse("domain", "member")
    val table = sys.props.getOrElse("table", "users")
    val configPath = sys.props.getOrElse("config", "member-pipeline/config/pipeline_config.yaml")

    val orch = new PipelineOrchestrator(projectId, domain, table, configPath)
    val p = orch.build()
    p.run()
  }
}