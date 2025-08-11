package com.analytics.framework.utils
import org.yaml.snakeyaml.Yaml
import scala.jdk.CollectionConverters._

object YamlLoader {
  private def toScala(v: Any): Any = v match {
    case m: java.util.Map[_, _] => m.asInstanceOf[java.util.Map[String, Any]].asScala.view.mapValues(toScala).toMap
    case l: java.util.List[_]   => l.asInstanceOf[java.util.List[Any]].asScala.map(toScala).toList
    case x => x
  }
  def load(path: String): Map[String, Any] = {
    val yaml = new Yaml()
    val txt = GcsFileIO.readText(path)
    val loaded = yaml.load[Any](txt)
    loaded match {
      case m: java.util.Map[_, _] => toScala(m).asInstanceOf[Map[String,Any]]
      case _ => Map.empty[String,Any]
    }
  }
}
