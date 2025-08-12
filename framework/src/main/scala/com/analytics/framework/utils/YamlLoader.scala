package com.analytics.framework.utils
import java.io.InputStreamReader
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import org.yaml.snakeyaml.Yaml

object YamlLoader {
  def load(path: String): Map[String, Any] = {
    val p = Paths.get(path)
    require(Files.exists(p), s"YAML not found: $path")
    val yaml = new Yaml()
    val is   = Files.newInputStream(p)
    val data = yaml.load[Any](new InputStreamReader(is, "UTF-8"))
    toScala(data).asInstanceOf[Map[String, Any]]
  }

  private def toScala(v: Any): Any = v match {
    case m: java.util.Map[_, _] =>
      m.asScala.map { case (k, vv) => k -> toScala(vv) }.toMap
    case l: java.util.List[_] =>
      l.asScala.map(toScala).toList
    case other => other
  }
}
