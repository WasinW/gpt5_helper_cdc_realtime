package com.analytics.framework.modules.quality

import org.apache.beam.sdk.transforms.DoFn
import com.analytics.framework.pipeline.core.CDCRecord
import scala.util.matching.Regex
import scala.jdk.CollectionConverters._

case class DQRule(name: String, columns: List[String], pattern: Option[String] = None, min: Option[Double] = None, max: Option[Double] = None, unique: Boolean = false)

class DataQualityEngine(rules: List[DQRule]) extends DoFn[CDCRecord, CDCRecord] {
  @org.apache.beam.sdk.transforms.DoFn.ProcessElement
  def process(ctx: DoFn[CDCRecord, CDCRecord]#ProcessContext): Unit = {
    val rec = ctx.element()
    var passed = true

    rules.foreach { r =>
      r.name match {
        case "not_null" =>
          passed &&= r.columns.forall(c => rec.data.containsKey(c) && rec.data.get(c) != null)
        case "regex" =>
          val re = r.pattern.map(_.r).getOrElse(".*".r)
          passed &&= r.columns.forall { c =>
            val v = Option(rec.data.get(c)).map(_.toString).getOrElse("")
            re.pattern.matcher(v).matches()
          }
        case "range" =>
          passed &&= r.columns.forall { c =>
            val v = Option(rec.data.get(c)).map(_.toString.toDouble).getOrElse(Double.NaN)
            !v.isNaN && r.min.forall(v >= _) && r.max.forall(v <= _)
          }
        case _ => ()
      }
    }

    if (passed) ctx.output(rec) // TODO: side output failed rows
  }
}