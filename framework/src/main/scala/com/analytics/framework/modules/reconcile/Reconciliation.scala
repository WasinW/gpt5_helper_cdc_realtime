package com.analytics.framework.modules.reconcile
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import scala.jdk.CollectionConverters._
import com.analytics.framework.connectors.S3SnapshotReader
import java.time.Instant
case class ReconcileInput(id: String, table: String, zone: String, gcpRow: java.util.Map[String, AnyRef])
case class ReconcileLog(recordId: String, table: String, zone: String, gcpCount: Long, awsCount: Long, countMatch: Boolean, diffs: List[Map[String, String]], ts: Long)
class ReconcileDoFn(bucket: String, prefix: String, idColumn: String, sampleLimit: Int = 5) extends DoFn[ReconcileInput, ReconcileLog] {
  @transient private var s3: S3SnapshotReader = _
  @DoFn.Setup def setup(): Unit = { s3 = new S3SnapshotReader(bucket, prefix); s3.init() }
  @DoFn.ProcessElement
  def process(ctx: DoFn[ReconcileInput, ReconcileLog]#ProcessContext, w: BoundedWindow): Unit = {
    val e = ctx.element()
    val keys = s3.listKeys(32); val rid = e.id
    val awsRows: List[Map[String, Any]] =
      keys.iterator.flatMap { k =>
        s3.readObjectLines(k).take(2000).flatMap { line =>
          import io.circe.parser._
          parse(line).toOption.flatMap(_.asObject).map(_.toMap.map { case (k,v) => k -> (if(v.isNull) null else v.toString)})
            .filter(_.getOrElse(idColumn, "") == rid)
        }
      }.take(sampleLimit).toList
    val gcpMap = e.gcpRow.asScala.toMap
    val gcpCount = 1L; val awsCount = awsRows.size.toLong; val countMatch = gcpCount == awsCount
    val diffs = awsRows.flatMap { r =>
      val cols = (r.keySet ++ gcpMap.keySet).filterNot(_.startsWith("_"))
      val bad = cols.filter(c => Option(gcpMap.getOrElse(c, null)).map(_.toString).orNull != Option(r.getOrElse(c, null)).map(_.toString).orNull)
      if (bad.nonEmpty) Some(Map("columns" -> bad.mkString(","))) else None
    }
    ctx.output(ReconcileLog(rid, e.table, e.zone, gcpCount, awsCount, countMatch, diffs, Instant.now().toEpochMilli))
  }
}
