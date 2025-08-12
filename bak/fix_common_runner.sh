#!/usr/bin/env bash
# This script replaces the CommonRunner.scala file with a version
# that does not depend on QualityRules. It also backs up the old
# CommonRunner.scala so you can restore it if needed.

set -euo pipefail

ROOT_DIR=$(git rev-parse --show-toplevel 2>/dev/null || echo ".")
FILE="$ROOT_DIR/framework/src/main/scala/com/analytics/framework/app/CommonRunner.scala"

if [ ! -f "$FILE" ]; then
  echo "Error: $FILE not found. Please run this script from the root of the repository." >&2
  exit 1
fi

# backup existing file with timestamp
cp "$FILE" "${FILE}.bak.$(date +%s)"

cat > "$FILE" <<'SCALA'
package com.analytics.framework.app

import com.analytics.framework.utils.{JavaInterop, YamlLoader}
import scala.collection.JavaConverters._
import com.analytics.framework.core.base.PipelineCtx
import com.analytics.framework.modules.audit.GcsAuditLogger
import com.analytics.framework.modules.quality.RulesLoader
import com.analytics.framework.pipeline.stages._
import com.analytics.framework.utils.{YamlLoader, RawConfigLoader}
import org.apache.beam.sdk.Pipeline

object CommonRunner {
  private def toStringMap(x: Any): Map[String, String] = x match {
    case m: Map[_, _] =>
      m.asInstanceOf[Map[String, Any]].map { case (k, v) => k.toString -> String.valueOf(v) }
    case jm: java.util.Map[_, _] =>
      jm.asInstanceOf[java.util.Map[String, Any]].asScala.toMap.map { case (k, v) => k.toString -> String.valueOf(v) }
    case _ => Map.empty[String, String]
  }

  def main(args: Array[String]): Unit = {
    val projectId = sys.props.getOrElse("projectId", "demo")
    val region    = sys.props.getOrElse("region", "asia-southeast1")
    val domain    = sys.props.getOrElse("domain", "member")
    val windowId  = sys.props.getOrElse("window_id", "202501010000")
    val configPath = "business-domains/member/resources/pipeline_config.yaml"

    // Load YAML config as a Scala Map
    val cfg: Map[String, Any] = YamlLoader.load(configPath)

    val datasets: Map[String, String] =
      cfg.get("datasets")
        .map(JavaInterop.deepAsScala)
        .map(toStringMap)
        .getOrElse(Map.empty)

    val buckets: Map[String, String] =
      cfg.get("buckets")
        .map(JavaInterop.deepAsScala)
        .map(toStringMap)
        .getOrElse(Map("audit" -> "gs://dummy-bucket"))

    implicit val ctx: PipelineCtx =
      PipelineCtx(
        projectId,
        region,
        domain,
        datasets,
        buckets,
        cfg.getOrElse("window_id_pattern", "yyyyMMddHHmm").toString,
        configPath,
        Map("window_id" -> windowId)
      )

    def audit(kind: String, zone: String) = (line: String) =>
      GcsAuditLogger.writeLine(s"${buckets("audit")}/" + s"$kind/$domain/$zone/$windowId.log", line)

    val p = Pipeline.create()

    // Example quality stage: pass not-null rules directly to QualityStage
    val _qualityLink =
      new QualityStage[Map[String, Any]](
        RulesLoader.loadNotNullRules("business-domains/member/resources/quality_rules.yaml"),
        audit("data_quality_log", "raw")
      )

    // Example refined stage: write refined records to BigQuery
    val _2 = new BqWriteStage(datasets("refined"), "member_profile")

    // Example reconcile stage: reconcile refined records against reference
    val _recLink =
      new ReconcileStage[Map[String, Any]](
        "refined",
        "member_profile",
        Map.empty,
        Nil,
        audit("reconcile_log", "refined")
      )

    p.run().waitUntilFinish()
  }
}
SCALA

echo "Updated $FILE successfully. A backup has been created in ${FILE}.bak.$(date +%s)."
