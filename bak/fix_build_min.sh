#!/usr/bin/env bash
set -euo pipefail

echo "== STEP 1: replace CollectionConverters =="
files=$(git grep -l -- 'scala.jdk.CollectionConverters' -- 'framework/**/*.scala' || true)
for f in $files; do
  sed -i -e 's#scala\.jdk\.CollectionConverters\._#scala.collection.JavaConverters._#g' "$f"
done

echo "== STEP 2: add DoFn.ProcessElement imports to stages (if missing) =="
inject() {
  f="$1"
  [ -f "$f" ] || return 0
  grep -q 'DoFn.ProcessElement' "$f" || \
    sed -i '/ParDo}/a import org.apache.beam.sdk.transforms.DoFn.ProcessElement' "$f" || true
}
for f in \
  framework/src/main/scala/com/analytics/framework/pipeline/stages/NotificationStage.scala \
  framework/src/main/scala/com/analytics/framework/pipeline/stages/MessageToRawStage.scala \
  framework/src/main/scala/com/analytics/framework/pipeline/stages/BqReadStage.scala \
  framework/src/main/scala/com/analytics/framework/pipeline/stages/BqWriteDynamicStage.scala \
  framework/src/main/scala/com/analytics/framework/pipeline/stages/FieldMapStage.scala \
  framework/src/main/scala/com/analytics/framework/pipeline/stages/FilterByTableStage.scala
do
  inject "$f"
done

echo "== STEP 3: fix TableDestination import (use Beam) =="
F=framework/src/main/scala/com/analytics/framework/pipeline/stages/BqWriteDynamicStage.scala
if [ -f "$F" ]; then
  sed -i 's#com.google.api.services.bigquery.model.{TableRow, TableSchema, TableDestination}#com.google.api.services.bigquery.model.{TableRow, TableSchema}\nimport org.apache.beam.sdk.io.gcp.bigquery.TableDestination#g' "$F" || true
fi

echo "== STEP 4: joda Duration API rename =="
PCORE=framework/src/main/scala/com/analytics/framework/pipeline/core/PipelineOrchestrator.scala
[ -f "$PCORE" ] && sed -i 's/Duration\.standardMillis(500)/Duration.millis(500)/g' "$PCORE" || true

echo "== STEP 5: ensure minimal core/utils/reconcile stubs (only if missing) =="
mk() { mkdir -p "$(dirname "$1")" && cat > "$1"; }

# PipelineCtx
if [ ! -f framework/src/main/scala/com/analytics/framework/core/base/PipelineCtx.scala ]; then
mk framework/src/main/scala/com/analytics/framework/core/base/PipelineCtx.scala <<'SCALA'
package com.analytics.framework.core.base
final case class PipelineCtx(
  projectId:String,
  region:String,
  domain:String,
  datasets: Map[String,String],
  buckets: Map[String,String],
  windowPattern:String,
  configPath:String,
  runtime: Map[String,String]
)
SCALA
fi

# BaseStage
if [ ! -f framework/src/main/scala/com/analytics/framework/core/base/BaseStage.scala ]; then
mk framework/src/main/scala/com/analytics/framework/core/base/BaseStage.scala <<'SCALA'
package com.analytics.framework.core.base
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
abstract class BaseStage[I,O]{
  def name:String
  def apply(p:Pipeline, in:PCollection[I])(implicit ctx:PipelineCtx): PCollection[O]
}
SCALA
fi

# YamlLoader
if [ ! -f framework/src/main/scala/com/analytics/framework/utils/YamlLoader.scala ]; then
mk framework/src/main/scala/com/analytics/framework/utils/YamlLoader.scala <<'SCALA'
package com.analytics.framework.utils
import java.io.InputStreamReader
import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConverters._
object YamlLoader{
  def load(path:String): Map[String,Any] = {
    val is = new java.io.FileInputStream(path)
    val yaml = new Yaml()
    try yaml.load[java.util.Map[String,Any]](new InputStreamReader(is,"UTF-8")).asScala.toMap
    finally is.close()
  }
}
SCALA
fi

# JsonDotPath
if [ ! -f framework/src/main/scala/com/analytics/framework/utils/JsonDotPath.scala ]; then
mk framework/src/main/scala/com/analytics/framework/utils/JsonDotPath.scala <<'SCALA'
package com.analytics.framework.utils
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
object JsonDotPath{
  private val om = new ObjectMapper().registerModule(DefaultScalaModule)
  def eval(json:String, path:String): Any = {
    val node = om.readTree(json)
    if (path=="$") return node
    val segs = path.stripPrefix("$.").split("\\.")
    var cur: JsonNode = node
    segs.foreach(s => if (cur!=null) cur = cur.path(s))
    if (cur==null || cur.isMissingNode) null
    else if (cur.isTextual) cur.asText()
    else if (cur.isNumber) cur.numberValue()
    else if (cur.isBoolean) cur.asBoolean()
    else om.writeValueAsString(cur)
  }
}
SCALA
fi

# ReconcileMappingLoader (อ่าน mapping เป็น Map[zone][table] -> Map[gcs_col -> s3_col])
if [ ! -f framework/src/main/scala/com/analytics/framework/modules/reconciliation/ReconcileMappingLoader.scala ]; then
mk framework/src/main/scala/com/analytics/framework/modules/reconciliation/ReconcileMappingLoader.scala <<'SCALA'
package com.analytics.framework.modules.reconciliation
import com.analytics.framework.utils.YamlLoader
import scala.collection.JavaConverters._
object ReconcileMappingLoader{
  def load(path:String): Map[String, Map[String, Map[String,String]]] = {
    val root = YamlLoader.load(path)
    // รองรับทั้งรูปแบบที่ wrap ใน key: mapping หรือ map ตรงๆ
    val raw = root.get("mapping").getOrElse(root)
    raw match {
      case m: java.util.Map[_,_] =>
        m.asInstanceOf[java.util.Map[String, java.util.Map[String, java.util.Map[String,String]]]]
         .asScala.map{ case (zone, mtables) =>
            zone -> mtables.asScala.map{ case (tbl, kv) => tbl -> kv.asScala.toMap }.toMap
         }.toMap
      case m: Map[_,_] =>
        m.asInstanceOf[Map[String, Map[String, Map[String,String]]]]
      case _ => Map.empty
    }
  }
  def get(path:String, zone:String, table:String): Map[String,String] =
    load(path).getOrElse(zone, Map.empty).getOrElse(table, Map.empty)
}
SCALA
fi

# S3SampleProvider (stub สำหรับ smoke test)
if [ ! -f framework/src/main/scala/com/analytics/framework/modules/reconciliation/S3SampleProvider.scala ]; then
mk framework/src/main/scala/com/analytics/framework/modules/reconciliation/S3SampleProvider.scala <<'SCALA'
package com.analytics.framework.modules.reconciliation
object S3SampleProvider{
  def fromS3(bucket:String, prefix:String, windowId:String): Iterable[Map[String,Any]] = Iterable.empty
}
SCALA
fi

echo "== STEP 6: ensure build.sbt deps (append once) =="
if ! grep -q 'jackson-module-scala' build.sbt ; then
cat >> build.sbt <<'SBT'

ThisBuild / libraryDependencies ++= Seq(
  "org.yaml" % "snakeyaml" % "2.3",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.1",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.1",
  "com.google.cloud" % "google-cloud-secretmanager" % "2.36.0",
  "software.amazon.awssdk" % "s3" % "2.25.43",
  "software.amazon.awssdk" % "auth" % "2.25.43",
  "io.circe" %% "circe-core" % "0.14.9",
  "io.circe" %% "circe-parser" % "0.14.9"
)
SBT
fi

echo "== DONE. Try compile =="
