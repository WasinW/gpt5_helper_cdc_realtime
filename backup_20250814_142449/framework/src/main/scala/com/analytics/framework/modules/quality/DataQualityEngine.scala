package com.analytics.framework.modules.quality

final case class NotNullRule(columns: List[String])

/** สตับ engine ให้คอมไพล์ผ่าน: ตรวจ not-null บน Scala Map */
object DataQualityEngine {
  def passedNotNull(rec: Map[String,Any], rule: NotNullRule): Boolean =
    rule.columns.forall(c => rec.get(c).exists(_ != null))
}
