package com.analytics.framework.utils
import java.io.InputStreamReader
import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConverters._
object YamlLoader{
  def load(path:String): Map[String,Any] = {
    val is = new java.io.FileInputStream(path)
    val yaml = new Yaml()
    try yaml.load[java.util.Map[String,Any]](new InputStreamReader(is,"UTF-8")).asScala.toMap
    finally is.close()
  }
}
