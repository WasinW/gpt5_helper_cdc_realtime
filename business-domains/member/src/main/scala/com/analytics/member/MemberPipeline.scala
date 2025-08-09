package com.analytics.member
import com.analytics.framework.pipeline.core.Orchestrator
object MemberPipeline {
  def main(args: Array[String]): Unit = {
    val projectId = sys.env.getOrElse("GCP_PROJECT", "demo-the-1")
    val configPath = sys.props.getOrElse("config", "business-domains/member/resources/pipeline_config.yaml")
    val p = new Orchestrator(projectId, configPath).build(); p.run()
  }
}
