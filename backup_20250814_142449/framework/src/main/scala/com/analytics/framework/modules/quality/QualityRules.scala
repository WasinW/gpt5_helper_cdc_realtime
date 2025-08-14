package com.analytics.framework.modules.quality
import com.analytics.framework.modules.quality.RulesLoader.Rule

case class QualityRules(
  notNull: Option[List[Rule[Map[String, Any]]]] = None
) {
  def all: List[Rule[Map[String, Any]]] = notNull.getOrElse(Nil)
}
