package com.analytics.framework.connectors

import scala.io.Source
import java.nio.file.{Files, Paths}
import com.google.gson.{JsonElement, JsonParser}
import scala.collection.JavaConverters._

/**
 * Reader for JSON Lines stored in S3.
 *
 * This stubbed implementation maps S3 URIs to relative filesystem paths by
 * removing the `s3://bucket/` prefix.  It then reads the file line by line,
 * parsing each non‑empty line as JSON via Gson and converting it into a
 * Scala `Map[String, Any]`.  If the file does not exist an empty list is
 * returned.
 */
object S3JsonlReader {
  /**
   * Read a JSON Lines file from an S3 URI.
   *
   * @param uri S3 URI (e.g. "s3://bucket/path/file.jsonl")
   * @return a list of records parsed into Scala maps
   */
  def read(uri: String): List[Map[String, Any]] = {
    val localPath = uri.replaceFirst("^s3://[^/]+/", "")
    val p         = Paths.get(localPath)
    if (!Files.exists(p)) return Nil
    val src = Source.fromFile(p.toFile)
    try {
      src.getLines().toList.flatMap { line =>
        val trimmed = line.trim
        if (trimmed.isEmpty) Nil
        else {
          try {
            val elem = JsonParser.parseString(trimmed)
            val rec  = toScala(elem).asInstanceOf[Map[String, Any]]
            List(rec)
          } catch {
            case _: Exception => Nil
          }
        }
      }
    } finally {
      src.close()
    }
  }

  /**
   * Backwards‑compatibility helper for legacy code.  Some components expect a
   * `readJsonLines` method on the reader that returns a sequence of maps.
   * This method simply delegates to [[read]] and widens the return type
   * to `Seq`.
   *
   * @param uri S3 URI pointing to a JSON Lines file
   * @return a sequence of records parsed into maps
   */
  def readJsonLines(uri: String): Seq[Map[String, Any]] = read(uri)

  private def toScala(elem: JsonElement): Any = {
    if (elem.isJsonNull) {
      null
    } else if (elem.isJsonPrimitive) {
      val prim = elem.getAsJsonPrimitive
      if (prim.isBoolean) prim.getAsBoolean
      else if (prim.isNumber) prim.getAsNumber
      else prim.getAsString
    } else if (elem.isJsonArray) {
      elem.getAsJsonArray.iterator().asScala.toList.map(e => toScala(e))
    } else if (elem.isJsonObject) {
      elem.getAsJsonObject.entrySet().asScala.map { entry =>
        entry.getKey -> toScala(entry.getValue)
      }.toMap
    } else {
      null
    }
  }
}
