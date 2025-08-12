package com.analytics.framework.utils
final case class RawIngestConfig()
object RawConfigLoader{
  def load(configPath:String): RawIngestConfig = RawIngestConfig()
}
