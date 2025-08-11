package com.analytics.framework.utils

import org.yaml.snakeyaml.Yaml
import java.io.{FileInputStream, InputStream}
import scala.collection.JavaConverters._

/**
  * Loads YAML configuration files into simple Scala data structures.
  * Configuration is kept intentionally generic; downstream code can
  * cast values into appropriate types.  See the sample configs
  * under `member-pipeline/config` for an example.
  */
object ConfigLoader {
  private val yaml = new Yaml()

  /**
    * Load a YAML file from the given path and convert it into a map.
    * Returns an empty map if the file cannot be read.
    */
  def loadDomainConfig(path: String): Map[String, Any] = {
    try {
      val input: InputStream = new FileInputStream(path)
      val data = yaml.load[java.util.Map[String, Any]](input)
      Option(data).map(_.asScala.toMap).getOrElse(Map.empty)
    } catch {
      case ex: Exception =>
        println(s"ConfigLoader: failed to load config from $path: ${ex.getMessage}")
        Map.empty
    }
  }
}