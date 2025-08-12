#!/usr/bin/env bash
set -euo pipefail

ROOT="$(pwd)"

# ---------- helpers ----------
write_file() {
  local path="$1"; shift
  mkdir -p "$(dirname "$path")"
  cat > "$path" <<'EOF'
'"$@"'
EOF
}

append_if_missing() {
  local file="$1"; local needle="$2"; local insert_block="$3"
  if ! grep -qF "$needle" "$file"; then
    printf "\n%s\n" "$insert_block" >> "$file"
  fi
}

# ---------- 1) build.sbt: ensure DirectRunner ----------
if [ -f "$ROOT/build.sbt" ]; then
  cp "$ROOT/build.sbt" "$ROOT/build.sbt.bak.$(date +%s)"
  # แทรก direct runner ต่อจาก core หากยังไม่มี
  if ! grep -q 'beam-runners-direct-java' "$ROOT/build.sbt"; then
    sed -i '0,/"beam-sdks-java-core"/ s//"org.apache.beam" % "beam-sdks-java-core" % versions.beam,\n  "org.apache.beam" % "beam-runners-direct-java" % versions.beam/' "$ROOT/build.sbt" || true
  fi
else
  cat > "$ROOT/build.sbt" <<'EOF'
ThisBuild / scalaVersion := "2.12.18"

lazy val versions = new {
  val beam      = "2.58.0"
  val gcloudBq  = "2.43.2"
  val slf4j     = "2.0.12"
  val gson      = "2.10.1"
  val snake     = "2.0"
  val awsSdk    = "2.25.62"
}

lazy val root = (project in file("."))
  .aggregate(framework, memberPipeline)
  .settings(
    name := "gpt5-helper-cdc-realtime",
    version := "0.3.0"
  )

lazy val commonLibs = Seq(
  "org.apache.beam" % "beam-sdks-java-core" % versions.beam,
  "org.apache.beam" % "beam-runners-direct-java" % versions.beam,
  "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % versions.beam,
  "com.google.cloud" % "google-cloud-bigquery" % versions.gcloudBq,
  "software.amazon.awssdk" % "s3" % versions.awsSdk,
  "com.google.code.gson" % "gson" % versions.gson,
  "org.yaml" % "snakeyaml" % versions.snake,
  "org.slf4j" % "slf4j-api" % versions.slf4j
)

lazy val framework = (project in file("framework")).settings(
  name := "cdc-framework",
  libraryDependencies ++= commonLibs
)

lazy val memberPipeline = (project in file("member-pipeline")).dependsOn(framework).settings(
  name := "member-pipeline"
)
EOF
fi

# ---------- 2) source files ----------

# 2.1 PipelineCtx
write_file "$ROOT/framework/src/main/scala/com/analytics/framework/core/base/PipelineCtx.scala" '
package com.analytics.framework.core.base

case class PipelineCtx(
  projectId: String,
  region: String,
  domain: String,
  datasets: Map[String, String],
  buckets: Map[String, String],
  windowIdPattern: String,
  configPath: String,
  runtimeArgs: Map[String, String]
) {
  def dataset(name: String): Option[String] = datasets.get(name)
  def bucket(name: String): Option[String]  = buckets.get(name)
  def arg(name: String): Option[String]     = runtimeArgs.get(name)
}
'

# 2.2 BaseStage
write_file "$ROOT/framework/src/main/scala/com/analytics/framework/pipeline/stages/BaseStage.scala" '
package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.PipelineCtx

trait BaseStage[I, O] {
  def name: String
  def run(ctx: PipelineCtx, in: Seq[I]): Seq[O]
}
'

# 2.3 TransformModule
write_file "$ROOT/framework/src/main/scala/com/analytics/framework/modules/transform/TransformModule.scala" '
package com.analytics.framework.modules.transform
import com.analytics.framework.core.base.PipelineCtx

trait TransformModule[I, O] {
  def transform(ctx: PipelineCtx, in: I): O
}
'

# 2.4 YamlLoader
write_file "$ROOT/framework/src/main/scala/com/analytics/framework/utils/YamlLoader.scala" '
package com.analytics.framework.utils
import java.io.InputStreamReader
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._
import org.yaml.snakeyaml.Yaml

object YamlLoader {
  def load(path: String): Map[String, Any] = {
    val p = Paths.get(path)
    require(Files.exists(p), s"YAML not found: $path")
    val yaml = new Yaml()
    val is   = Files.newInputStream(p)
    val data = yaml.load[Any](new InputStreamReader(is, "UTF-8"))
    toScalaMap(data).asInstanceOf[Map[String, Any]]
  }
  private def toScalaMap(v: Any): Any = v match {
    case m: java.util.Map[_, _] => m.asScala.view.mapValues(toScalaMap).toMap
    case l: java.util.List[_]   => l.asScala.map(toScalaMap).toList
    case other                  => other
  }
}
'

# 2.5 RawConfigLoader
write_file "$ROOT/framework/src/main/scala/com/analytics/framework/utils/RawConfigLoader.scala" '
package com.analytics.framework.utils
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._
import com.google.gson.{JsonElement, JsonParser}

object RawConfigLoader {
  def load(path: String): Map[String, Any] = {
    if (path.toLowerCase.endsWith(".yaml") || path.toLowerCase.endsWith(".yml")) {
      YamlLoader.load(path)
    } else if (path.toLowerCase.endsWith(".json")) {
      val json = new String(Files.readAllBytes(Paths.get(path)), "UTF-8")
      toScala(JsonParser.parseString(json))
    } else {
      throw new IllegalArgumentException(s"Unsupported config: $path")
    }
  }
  private def toScala(el: JsonElement): Map[String, Any] = {
    import scala.collection.mutable
    def rec(e: JsonElement): Any = {
      if (e.isJsonNull) null
      else if (e.isJsonPrimitive) {
        val p = e.getAsJsonPrimitive
        if (p.isBoolean) p.getAsBoolean
        else if (p.isNumber) p.getAsNumber
        else p.getAsString
      } else if (e.isJsonArray) e.getAsJsonArray.iterator().asScala.map(rec).toList
      else e.getAsJsonObject.entrySet().asScala.map(en => en.getKey -> rec(en.getValue)).toMap
    }
    rec(el).asInstanceOf[Map[String, Any]]
  }
}
'

# 2.6 JsonDotPath
write_file "$ROOT/framework/src/main/scala/com/analytics/framework/utils/JsonDotPath.scala" '
package com.analytics.framework.utils
import com.google.gson.JsonParser
import scala.jdk.CollectionConverters._

object JsonDotPath {
  def extract(json: String, path: String): Option[Any] = {
    val root = JsonParser.parseString(json).getAsJsonObject
    var cur  = Option(root: Any)
    for (key <- path.split("\\.")) {
      cur = cur.flatMap {
        case obj: com.google.gson.JsonObject if obj.has(key) =>
          val v = obj.get(key)
          if (v.isJsonPrimitive) Some(if (v.getAsJsonPrimitive.isBoolean) v.getAsBoolean
            else if (v.getAsJsonPrimitive.isNumber) v.getAsNumber
            else v.getAsString)
          else if (v.isJsonObject) Some(v.getAsJsonObject)
          else if (v.isJsonArray) Some(v.getAsJsonArray.asScala.toList)
          else None
        case _ => None
      }
    }
    cur
  }
}
'

# 2.7 RulesLoader
write_file "$ROOT/framework/src/main/scala/com/analytics/framework/modules/quality/RulesLoader.scala" '
package com.analytics.framework.modules.quality
import com.analytics.framework.utils.YamlLoader

object RulesLoader {
  // rule = return List of failure messages (empty list = pass)
  type Rule[T] = T => List[String]

  def loadNotNullRules(path: String): List[Rule[Map[String, Any]]] = {
    val cfg = YamlLoader.load(path)
    val fields: List[String] = cfg.get("not_null") match {
      case Some(xs: Iterable[_]) => xs.collect { case s: String => s }.toList
      case Some(other)           => throw new IllegalArgumentException(s"not_null must be list of strings, got $other")
      case None                  => Nil
    }
    fields.map { f =>
      (m: Map[String, Any]) =>
        val ok = m.get(f).exists(v => v != null && (v.toString.trim.nonEmpty))
        if (ok) Nil else List(s"NOT_NULL_FAIL:$f")
    }
  }
}
'

# 2.8 QualityStage
write_file "$ROOT/framework/src/main/scala/com/analytics/framework/pipeline/stages/QualityStage.scala" '
package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.PipelineCtx
import com.analytics.framework.modules.quality.RulesLoader.Rule

class QualityStage[T](rules: List[Rule[T]], audit: String => Unit) extends BaseStage[T, T] {
  override def name: String = "QualityStage"
  override def run(ctx: PipelineCtx, in: Seq[T]): Seq[T] = {
    in.foreach { rec =>
      val fails = rules.flatMap(r => r(rec))
      if (fails.nonEmpty) audit(fails.mkString("|"))
    }
    in
  }
}
'

# 2.9 ReconcileStage
write_file "$ROOT/framework/src/main/scala/com/analytics/framework/pipeline/stages/ReconcileStage.scala" '
package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.PipelineCtx
import com.analytics.framework.connectors.S3JsonlReader

case class ReconCfg(zone: String, table: String, options: Map[String, String])

class ReconcileStage[T <: Map[String, Any]](
  cfg: ReconCfg,
  primaryKeys: List[String],
  audit: String => Unit
) extends BaseStage[T, T] {
  override def name: String = "ReconcileStage"
  override def run(ctx: PipelineCtx, in: Seq[T]): Seq[T] = {
    // ตัวอย่าง: อ่านไฟล์อ้างอิงจาก S3 (JSONL) แล้ว log ถ้าหา key ไม่เจอ
    val s3uri  = cfg.options.getOrElse("s3_uri", "")
    val ref    = if (s3uri.nonEmpty) S3JsonlReader.readJsonLines(s3uri) else Seq.empty[Map[String, Any]]
    val refIdx = ref.groupBy(r => primaryKeys.map(k => r.getOrElse(k, null)).mkString("||"))

    in.foreach { rec =>
      val key = primaryKeys.map(k => rec.getOrElse(k, null)).mkString("||")
      if (!refIdx.contains(key)) audit(s"RECON_MISSING:$key")
    }
    in
  }
}
'

# 2.10 TransformStage
write_file "$ROOT/framework/src/main/scala/com/analytics/framework/pipeline/stages/TransformStage.scala" '
package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.PipelineCtx
import com.analytics.framework.modules.transform.TransformModule

class TransformStage[I, O](module: TransformModule[I, O]) extends BaseStage[I, O] {
  override def name: String = s"TransformStage(${module.getClass.getSimpleName})"
  override def run(ctx: PipelineCtx, in: Seq[I]): Seq[O] = in.map(x => module.transform(ctx, x))
}
'

# 2.11 S3JsonlReader
write_file "$ROOT/framework/src/main/scala/com/analytics/framework/connectors/S3JsonlReader.scala" '
package com.analytics.framework.connectors
import scala.io.Source
import java.nio.file.{Files, Paths}
import scala.util.Using
import com.google.gson.JsonParser
import scala.jdk.CollectionConverters._

object S3JsonlReader {
  // สำหรับ local dev: map s3://bucket/key -> ./_s3_mirror/bucket/key
  private def mapLocal(uri: String): String = {
    if (uri.startsWith("s3://")) {
      val no = uri.stripPrefix("s3://")
      s"_s3_mirror/$no"
    } else uri
  }
  def readJsonLines(uri: String): Seq[Map[String, Any]] = {
    val p = Paths.get(mapLocal(uri))
    if (!Files.exists(p)) return Seq.empty
    Using.resource(Source.fromFile(p.toFile, "UTF-8")) { src =>
      src.getLines().toSeq.flatMap { line =>
        if (line.trim.isEmpty) Nil
        else {
          val obj = JsonParser.parseString(line).getAsJsonObject
          val mp  = obj.entrySet().asScala.map(e => e.getKey -> {
            val v = e.getValue
            if (v.isJsonNull) null
            else if (v.isJsonPrimitive) {
              val p = v.getAsJsonPrimitive
              if (p.isBoolean) p.getAsBoolean
              else if (p.isNumber) p.getAsNumber
              else p.getAsString
            } else v.toString
          }).toMap
          Seq(mp)
        }
      }
    }
  }
}
'

# 2.12 SecretManager
write_file "$ROOT/framework/src/main/scala/com/analytics/framework/connectors/SecretManager.scala" '
package com.analytics.framework.connectors
object SecretManager {
  def getSecret(name: String): String = sys.env.getOrElse(name, "")
}
'

# 2.13 S3Connector (stub)
write_file "$ROOT/framework/src/main/scala/com/analytics/framework/connectors/S3Connector.scala" '
package com.analytics.framework.connectors
import java.nio.file.{Files, Paths}

class S3Connector {
  def downloadToLocal(s3Uri: String, dest: String): Unit = {
    // stub: สำหรับ local dev ไม่โหลดจริง
    val _ = (s3Uri, dest)
  }
}
'

# 2.14 BqWriteStage (stub)
write_file "$ROOT/framework/src/main/scala/com/analytics/framework/pipeline/stages/BqWriteStage.scala" '
package com.analytics.framework.pipeline.stages
import com.analytics.framework.core.base.PipelineCtx

class BqWriteStage(dataset: String, table: String) extends BaseStage[Map[String, Any], Map[String, Any]] {
  override def name: String = s"BqWriteStage($dataset.$table)"
  override def run(ctx: PipelineCtx, in: Seq[Map[String, Any]]): Seq[Map[String, Any]] = {
    println(s"[BQ WRITE] ${in.size} rows -> $dataset.$table")
    in
  }
}
'

# 2.15 GcsAuditLogger
write_file "$ROOT/framework/src/main/scala/com/analytics/framework/modules/audit/GcsAuditLogger.scala" '
package com.analytics.framework.modules.audit
import java.io.{File, FileWriter, BufferedWriter}

object GcsAuditLogger {
  def writeLine(path: String, line: String): Unit = {
    if (path.startsWith("gs://")) {
      // local dev: แค่พิมพ์ออกจอ
      println(s"[AUDIT] $path :: $line")
    } else {
      val f = new File(path)
      f.getParentFile.mkdirs()
      val bw = new BufferedWriter(new FileWriter(f, true))
      try { bw.write(line); bw.newLine() } finally bw.close()
    }
  }
}
'

# ---------- 3) quality rules sample (ถ้าไม่มี) ----------
mkdir -p "$ROOT/business-domains/member/resources"
if [ ! -f "$ROOT/business-domains/member/resources/quality_rules.yaml" ]; then
  cat > "$ROOT/business-domains/member/resources/quality_rules.yaml" <<'EOF'
not_null:
  - member_id
  - updated_at
EOF
fi

echo "Done. Files created/updated. Backup of build.sbt kept if existed."
