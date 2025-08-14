package com.analytics.framework.core.base

import java.util.{List => JList}

/** โมดูลทรานส์ฟอร์มแบบ common ให้โดเมน implements ได้ง่าย ๆ */
trait TransformModule[I, O] {
  /** คืนค่ากลับเป็น JList เพื่อให้ง่ายต่อการใช้กับ Beam/JVM ทั่วไป */
  def transform(records: JList[I], params: Map[String, Any])(implicit ctx: PipelineCtx): JList[O]
}
