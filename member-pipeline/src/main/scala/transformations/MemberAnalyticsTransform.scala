package com.analytics.member.transformations

import com.analytics.framework.pipeline.transformation.AnalyticsTransformation

/**
  * Domain‑specific analytics transformation for the Member domain.  Use
  * this class to compute aggregated metrics and KPIs relevant to
  * member analytics.
  */
class MemberAnalyticsTransform extends AnalyticsTransformation {
  override def transform(refinedRecords: Any): Any = {
    println("MemberAnalyticsTransform: computing member analytics metrics")
    super.transform(refinedRecords)
  }
}