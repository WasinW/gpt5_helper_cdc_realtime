package com.analytics.framework.utils
import org.yaml.snakeyaml.Yaml
import java.io.FileInputStream
import scala.jdk.CollectionConverters._
import java.util.{Map => JMap}
object ConfigLoader {
  def load(path: String): JMap[String, Object] = {
    val yaml = new Yaml()
    val in = new FileInputStream(path)
    try yaml.load(in).asInstanceOf[JMap[String, Object]]
    finally in.close()
  }
}
