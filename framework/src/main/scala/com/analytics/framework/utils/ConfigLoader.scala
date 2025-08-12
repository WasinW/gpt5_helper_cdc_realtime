package com.analytics.framework.utils

import org.yaml.snakeyaml.Yaml
import java.io.FileInputStream
import scala.collection.JavaConverters._

object ConfigLoader {
  def load(path: String): java.util.Map[String, Object] = {
    val yaml = new Yaml()
    val in = new FileInputStream(path)
    try yaml.load(in).asInstanceOf[java.util.Map[String, Object]]
    finally in.close()
  }
}