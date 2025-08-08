package com.analytics.framework.transform

import org.apache.beam.sdk.transforms.DoFn
import com.analytics.framework.pipeline.core.CDCRecord
import scala.jdk.CollectionConverters._

class StructureTransformer(mapping: Map[String, String]) extends DoFn[CDCRecord, CDCRecord] {
  @org.apache.beam.sdk.transforms.DoFn.ProcessElement
  def process(ctx: DoFn[CDCRecord, CDCRecord]#ProcessContext): Unit = {
    val rec = ctx.element()
    val data = new java.util.HashMap[String, AnyRef](rec.data)
    mapping.foreach { case (col, typ) =>
      if (data.containsKey(col) && data.get(col) != null) {
        val v = data.get(col).toString
        typ.toLowerCase match {
          case "string" => data.put(col, v)
          case "int64"  => data.put(col, Long.box(v.toLong))
          case "float64"=> data.put(col, Double.box(v.toDouble))
          case "bool"   => data.put(col, Boolean.box(v.equalsIgnoreCase("true")))
          case _        => data.put(col, v)
        }
      }
    }
    ctx.output(rec.copy(data = data, zone = "structure"))
  }
}