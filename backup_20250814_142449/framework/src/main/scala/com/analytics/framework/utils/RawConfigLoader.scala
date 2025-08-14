package com.analytics.framework.utils
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import com.google.gson.{JsonElement, JsonParser}

object RawConfigLoader {
  def load(path: String): Map[String, Any] = {
    if (path.toLowerCase.endsWith(".yaml") || path.toLowerCase.endsWith(".yml")) {
      YamlLoader.load(path)
    } else if (path.toLowerCase.endsWith(".json")) {
      val json = new String(Files.readAllBytes(Paths.get(path)), "UTF-8")
      toScala(JsonParser.parseString(json))
    } else {
      throw new IllegalArgumentException(s"Unsupported config: $path")
    }
  }

  private def toScala(el: JsonElement): Map[String, Any] = {
    def rec(e: JsonElement): Any = {
      if (e.isJsonNull) null
      else if (e.isJsonPrimitive) {
        val p = e.getAsJsonPrimitive
        if (p.isBoolean) p.getAsBoolean
        else if (p.isNumber) p.getAsNumber
        else p.getAsString
      } else if (e.isJsonArray) {
        e.getAsJsonArray.iterator().asScala.map(x => rec(x)).toList
      } else {
        e.getAsJsonObject.entrySet().asScala.map(en => en.getKey -> rec(en.getValue)).toMap
      }
    }
    rec(el).asInstanceOf[Map[String, Any]]
  }
}
