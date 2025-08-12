package com.analytics.framework.utils
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
object JsonDotPath{
  private val om = new ObjectMapper()
  def eval(json:String, path:String): Any = {
    val node = om.readTree(json)
    if (path=="$") return node
    val segs = path.stripPrefix("$.").split("\\.")
    var cur: JsonNode = node
    segs.foreach(s => if (cur!=null) cur = cur.path(s))
    if (cur==null || cur.isMissingNode) null
    else if (cur.isTextual) cur.asText()
    else if (cur.isNumber) cur.numberValue()
    else if (cur.isBoolean) java.lang.Boolean.valueOf(cur.asBoolean())
    else om.writeValueAsString(cur)
  }
}
