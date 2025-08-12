package com.analytics.framework.utils
import com.google.gson.{JsonElement, JsonObject, JsonParser}
import scala.collection.JavaConverters._

object JsonDotPath {
  def extract(json: String, path: String): Option[Any] = {
    val root = JsonParser.parseString(json).getAsJsonObject
    var cur: Option[Any] = Some(root)
    for (key <- path.split("\\.")) {
      cur = cur.flatMap {
        case obj: JsonObject if obj.has(key) =>
          val v = obj.get(key)
          if (v.isJsonPrimitive) {
            val p = v.getAsJsonPrimitive
            Some(if (p.isBoolean) p.getAsBoolean else if (p.isNumber) p.getAsNumber else p.getAsString)
          } else if (v.isJsonObject) Some(v.getAsJsonObject)
          else if (v.isJsonArray) Some(v.getAsJsonArray.iterator().asScala.toList)
          else None
        case _ => None
      }
    }
    cur
  }

  // ให้เข้ากันกับโค้ดเดิมที่เรียก JsonDotPath.eval(...)
  def eval(json: String, path: String): Option[Any] = extract(json, path)
}
