package com.analytics.framework.core.base

case class PipelineCtx(
  projectId: String,
  region: String,
  domain: String,
  datasets: Map[String, String],
  buckets: Map[String, String],
  windowIdPattern: String,
  configPath: String,
  runtimeArgs: Map[String, String]
) {
  def dataset(name: String): Option[String] = datasets.get(name)
  def bucket(name: String): Option[String]  = buckets.get(name)
  def arg(name: String): Option[String]     = runtimeArgs.get(name)
}
