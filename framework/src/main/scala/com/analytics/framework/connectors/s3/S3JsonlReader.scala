package com.analytics.framework.connectors.s3
import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.JavaConverters._
class S3JsonlReader {
  private val om = new ObjectMapper()
  def toMap(line:String): Map[String,Any] = {
    val m = om.readValue(line, classOf[java.util.Map[String,Object]])
    m.asScala.toMap
  }
  private def toDate(windowId:String): String = {
    val Re = raw"(\\d{4})(\\d{2})(\\d{2}).*".r
    windowId match {
      case Re(y,m,d) => s"$y-$m-$d"
      case _ => windowId
    }
  }
}
