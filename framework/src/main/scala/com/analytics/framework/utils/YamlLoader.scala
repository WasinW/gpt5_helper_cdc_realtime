package com.analytics.framework.utils
import org.yaml.snakeyaml.Yaml
import java.nio.file.{Files, Paths}
import java.io.InputStream

object YamlLoader {
  private val yaml = new Yaml()
  def load(path: String): Map[String, Any] = {
    val is: InputStream = Files.newInputStream(Paths.get(path))
    try {
      val raw: Any = yaml.load(is) // java.util types
      JavaInterop.deepMap(raw)     // convert all levels to Scala
    } finally {
      is.close()
    }
  }
}
