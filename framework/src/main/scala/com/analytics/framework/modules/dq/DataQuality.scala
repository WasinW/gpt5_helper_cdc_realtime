package com.analytics.framework.modules.dq
import org.apache.beam.sdk.transforms.DoFn
import com.analytics.framework.pipeline.model.CDCRecord
import scala.jdk.CollectionConverters._
sealed trait Rule
case class NotNull(columns: List[String]) extends Rule
case class RegexRule(column: String, pattern: String) extends Rule
case class RangeRule(column: String, min: Double, max: Double) extends Rule
case class DQResult(passed: Boolean, rule: String, detail: String)
class DataQualityDoFn(rules: List[Rule]) extends DoFn[CDCRecord, (CDCRecord, DQResult)] {
  @DoFn.ProcessElement
  def process(ctx: DoFn[CDCRecord, (CDCRecord, DQResult)]#ProcessContext): Unit = {
    val rec = ctx.element(); val data = rec.data.asScala.toMap
    var ok = true; val details = scala.collection.mutable.ListBuffer[String]()
    rules.foreach {
      case NotNull(cols) =>
        val bad = cols.filter(c => !data.contains(c) || data(c) == null)
        if (bad.nonEmpty) { ok = false; details += s"not_null_missing=${bad.mkString(",")}" }
      case RegexRule(col, pat) =>
        val v = data.get(col).map(_.toString).getOrElse("")
        if (!v.matches(pat)) { ok = false; details += s"regex_failed=$col" }
      case RangeRule(col, min, max) =>
        val d = data.get(col).map(_.toString.toDoubleOption.getOrElse(Double.NaN)).getOrElse(Double.NaN)
        if (d.isNaN || d < min || d > max) { ok = false; details += s"range_failed=$col" }
    }
    ctx.output((rec, DQResult(ok, "combined", details.mkString(";"))))
  }
}
