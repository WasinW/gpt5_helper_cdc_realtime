package com.analytics.framework.modules.reconciliation
import scala.collection.JavaConverters._
final case class ReconcileRecord(data: java.util.Map[String,AnyRef])
final case class ReconcileResult(status:String, mismatches: List[String], mismatchSamples: List[String])
class ReconciliationEngine(){
  def reconcile(gcp: ReconcileRecord, aws: Map[String,Any], mapping: Map[String,String]): ReconcileResult = {
    val gcpData = gcp.data.asScala.toMap
    // very naive check
    val mism = mapping.flatMap{ case (gcpCol, s3Col) =>
      val gv = gcpData.getOrElse(gcpCol, null)
      val av = aws.getOrElse(s3Col, null)
      if (Option(gv).map(_.toString).orNull == Option(av).map(_.toString).orNull) None
      else Some(s"$gcpCol != $s3Col")
    }.toList
    ReconcileResult(if (mism.isEmpty) "OK" else "MISMATCH", mism, Nil)
  }
}
