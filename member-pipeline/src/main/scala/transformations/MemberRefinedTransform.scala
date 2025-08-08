package com.analytics.member.transformations

import com.analytics.framework.pipeline.transformation.RefinedTransformation

/**
  * Domain‑specific transformation for the Member refined zone.  Here
  * you would implement logic such as joins across member tables,
  * calculation of derived attributes and business rules.
  */
class MemberRefinedTransform extends RefinedTransformation {
  override def transform(rawRecords: Any): Any = {
    println("MemberRefinedTransform: custom refining logic for Member domain")
    super.transform(rawRecords)
  }
}