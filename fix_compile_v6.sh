#!/usr/bin/env bash
set -euo pipefail

write(){ mkdir -p "$(dirname "$1")"; cat > "$1"; }

# (A) CDCRecord: เพิ่ม zone เป็น optional field (default "")
write framework/src/main/scala/com/analytics/framework/pipeline/core/CDCDetector.scala <<'SCALA'
package com.analytics.framework.pipeline.core
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

final case class CDCRecord(data: Map[String,Any], zone: String = "")

class CDCDetector extends DoFn[Map[String,Any], CDCRecord] {
  @ProcessElement
  def process(ctx: DoFn[Map[String,Any], CDCRecord]#ProcessContext): Unit = {
    // stub: pass-through wrap to CDCRecord
    val m = ctx.element()
    ctx.output(CDCRecord(m))
  }
}
SCALA

# (B) DataQualityEngine: ใช้ Scala Map (ไม่ใช้ containsKey / &&=)
write framework/src/main/scala/com/analytics/framework/modules/quality/DataQualityEngine.scala <<'SCALA'
package com.analytics.framework.modules.quality

final case class NotNullRule(columns: List[String])

/** สตับ engine ให้คอมไพล์ผ่าน: ตรวจ not-null บน Scala Map */
object DataQualityEngine {
  def passedNotNull(rec: Map[String,Any], rule: NotNullRule): Boolean =
    rule.columns.forall(c => rec.get(c).exists(_ != null))
}
SCALA

# (C) RawInserter: สตับ DoFn เขียนออกต่อด้วย zone="raw" (ตัดการแปลงเป็น java.util.Map ออก)
write framework/src/main/scala/com/analytics/framework/pipeline/core/RawInserter.scala <<'SCALA'
package com.analytics.framework.pipeline.core
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

class RawInserter extends DoFn[CDCRecord, CDCRecord] {
  @ProcessElement
  def process(ctx: DoFn[CDCRecord, CDCRecord]#ProcessContext): Unit = {
    val rec = ctx.element()
    // stub: ถือว่า insert สำเร็จ แล้ว forward พร้อม zone = raw
    ctx.output(rec.copy(zone = "raw"))
  }
}
SCALA

# (D) StructureTransformer: สตับ DoFn แค่เปลี่ยน zone = "structure"
write framework/src/main/scala/com/analytics/framework/transform/StructureTransformer.scala <<'SCALA'
package com.analytics.framework.transform
import com.analytics.framework.pipeline.core.CDCRecord
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

class StructureTransformer extends DoFn[CDCRecord, CDCRecord] {
  @ProcessElement
  def process(ctx: DoFn[CDCRecord, CDCRecord]#ProcessContext): Unit = {
    val rec = ctx.element()
    ctx.output(rec.copy(zone = "structure"))
  }
}
SCALA

echo "== v6 files written =="
