package com.analytics.framework.core.base
final case class PipelineCtx(
  projectId:String,
  region:String,
  domain:String,
  datasets: Map[String,String],
  buckets: Map[String,String],
  windowPattern:String,
  configPath:String,
  runtime: Map[String,String]
)
