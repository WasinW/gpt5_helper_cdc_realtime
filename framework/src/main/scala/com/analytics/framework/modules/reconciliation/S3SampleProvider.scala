package com.analytics.framework.modules.reconciliation
import com.analytics.framework.connectors.s3.S3JsonlReader
object S3SampleProvider {
  /** Returns supplier for ReconcileStage.s3Sample. It fetches S3 rows then (optionally) keeps only columns used in mapping (S3 names). */
  def build(projectId:String, cfg: Map[String,Any], zone:String, table:String, windowId:String, gcsToS3: Map[String,String]): () => Iterable[Map[String,Any]] = {
    () => {
      val all = S3JsonlReader.readJsonlForTable(projectId, cfg, zone, table, windowId)
      if (gcsToS3.isEmpty) all
      else {
        val s3Cols = gcsToS3.values.toSet
        all.map(r => r.filter{ case (k,_) => s3Cols.contains(k) })
      }
    }
  }
}
