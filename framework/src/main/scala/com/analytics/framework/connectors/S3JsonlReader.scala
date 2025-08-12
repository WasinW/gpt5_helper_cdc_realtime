package com.analytics.framework.connectors
import scala.io.Source
import java.nio.file.{Files, Paths}
import com.google.gson.JsonParser
import scala.collection.JavaConverters._

object S3JsonlReader {
  private def mapLocal(uri: String): String = {
    if (uri.startsWith("s3://")) {
      val no = uri.stripPrefix("s3://")
      s"_s3_mirror/$no"
    } else uri
  }

  def readJsonLines(uri: String): Seq[Map[String, Any]] = {
    val p = Paths.get(mapLocal(uri))
    if (!Files.exists(p)) return Seq.empty
    val src = Source.fromFile(p.toFile, "UTF-8")
    try {
      src.getLines().toSeq.flatMap { line =>
        if (line.trim.isEmpty) Nil
        else {
          val obj = JsonParser.parseString(line).getAsJsonObject
          val mp  = obj.entrySet().asScala.map(e => e.getKey -> {
            val v = e.getValue
            if (v.isJsonNull) null
            else if (v.isJsonPrimitive) {
              val p = v.getAsJsonPrimitive
              if (p.isBoolean) p.getAsBoolean
              else if (p.isNumber) p.getAsNumber
              else p.getAsString
            } else v.toString
          }).toMap
          Seq(mp)
        }
      }
    } finally {
      src.close()
    }
  }
}
