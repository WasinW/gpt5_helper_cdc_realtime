package com.analytics.framework.pipeline.stages

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.transforms.{DoFn, ParDo, View, Count}
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, FinishBundle}
import com.analytics.framework.core.base.PipelineCtx
import scala.collection.JavaConverters._

/**
 * @param zone       "structure" | "refined" | "analytics"
 * @param table      table name
 * @param g2sMapping mapping of GCP-field -> S3-field (for value comparisons). Must include an id mapping (e.g. "id"->"id")
 * @param sample     up to N sample records from S3 snapshot (already as Scala maps)
 * @param audit      side-effect logger (e.g., write to GCS/GCS logs)
 */
class ReconcileStage[T <: Map[String,Any]]
  (zone:String,
   table:String,
   g2sMapping: Map[String,String],
   sample: List[Map[String,Any]],
   audit: String => Unit
  ) {

  private val idKey: Option[(String,String)] =
    g2sMapping.find { case (g,s) => g.equalsIgnoreCase("id") || g.toLowerCase.endsWith("_id") }

  def apply(p: Pipeline, in: PCollection[Map[String,Any]])(implicit ctx: PipelineCtx): PCollection[Map[String,Any]] = {
    // Build side-input: sample map keyed by S3 id field (if mapping provided)
    val s3IdKey = idKey.map(_._2).getOrElse("id")
    val s3Index: Map[Any, Map[String,Any]] = sample.flatMap { m =>
      m.get(s3IdKey).map(_ -> m)
    }.toMap

    val s3Side = p.apply("CreateS3Index", org.apache.beam.sdk.transforms.Create.of(s3Index))
      .apply("SingletonS3Index", View.asSingleton())

    val mappingBC = g2sMapping

    val out = in.apply("Reconcile per record", ParDo.of(new DoFn[Map[String,Any], Map[String,Any]] {
      private var mismatches: Int = 0
      private var checked: Int = 0
      private var matched: Int = 0
      private val maxSamples = 20
      private val samples = scala.collection.mutable.ListBuffer.empty[String]

      @ProcessElement
      def proc(c: DoFn[Map[String,Any], Map[String,Any]]#ProcessContext): Unit = {
        val rec = c.element()
        checked += 1
        val idx = c.sideInput(s3Side).asInstanceOf[java.util.Map[Any, Map[String,Any]]]
        val scalaIdx: Map[Any, Map[String,Any]] = idx.asInstanceOf[java.util.Map[Any, Map[String,Any]]].asScala.toMap

        val gIdKey = idKey.map(_._1).getOrElse("id")
        val gid    = rec.getOrElse(gIdKey, null)

        scalaIdx.get(gid) match {
          case Some(s3rec) =>
            val diffs = mappingBC.flatMap{ case (g,s) =>
              val gv = rec.getOrElse(g, null)
              val sv = s3rec.getOrElse(s, null)
              if (String.valueOf(gv) != String.valueOf(sv))
                Some(s"$g!=$s (gcp=$gv, s3=$sv)")
              else None
            }
            if (diffs.nonEmpty) {
              mismatches += 1
              if (samples.size < maxSamples) {
                // 1) กรณีมีทั้ง gcp และ s3 แล้วพบ diff -> เก็บตัวอย่าง
                samples += s"""{"id":"$gid","diffs":"${diffs.mkString(";")}","gcp":${rec.toString()},"s3":${s3rec.toString()}}"""
              }
            } else {
              matched += 1
            }
          case None =>
            mismatches += 1
            if (samples.size < maxSamples) {
              // 2) กรณีหา record บน s3 ไม่เจอ -> เก็บตัวอย่างว่า missing_on_s3
              samples += s"""{"id":"$gid","diffs":"missing_on_s3"}"""

            }
        }
        c.output(rec)
      }

      @FinishBundle
      def finish(c: DoFn[Map[String,Any], Map[String,Any]]#FinishBundleContext): Unit = {
        audit( s"""{"level":"INFO","type":"reconcile","zone":"$zone","table":"$table",
              "checked":$checked,"matched":$matched,"mismatches":$mismatches,
              "mapping":"${mappingBC.mkString(",")}",
              "samples":[${samples.mkString(",")}]}"""
        )
      }
    }).withSideInputs(s3Side))

    out
  }
}
