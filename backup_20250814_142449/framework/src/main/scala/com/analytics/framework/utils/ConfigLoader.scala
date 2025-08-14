package com.analytics.framework.utils

object ConfigLoader {
  def load(path: String): Map[String, Any] = YamlLoader.load(path)
}
