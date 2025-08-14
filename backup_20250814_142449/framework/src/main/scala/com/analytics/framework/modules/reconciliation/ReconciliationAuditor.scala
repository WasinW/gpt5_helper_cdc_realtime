package com.analytics.framework.modules.reconciliation
import scala.collection.JavaConverters._
class ReconciliationAuditor {
  def toMap(r: ReconcileResult): java.util.Map[String,AnyRef] = {
    Map(
      "status" -> r.status,
      "mismatches" -> r.mismatches.asJava,
      "mismatch_samples" -> r.mismatchSamples.asJava
    ).asJava.asInstanceOf[java.util.Map[String, AnyRef]]
  }
}
