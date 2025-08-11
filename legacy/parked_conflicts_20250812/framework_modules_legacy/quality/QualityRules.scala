package com.analytics.framework.modules.quality

/**
  * Defines a set of data quality rules.  Rules can be loaded
  * dynamically from configuration.  For example, you might define
  * not‑null constraints, value ranges or regular expression
  * validations.  This stub provides a default empty rule set.
  */
class QualityRules {
  def validate(record: Map[String, Any]): Boolean = true
}