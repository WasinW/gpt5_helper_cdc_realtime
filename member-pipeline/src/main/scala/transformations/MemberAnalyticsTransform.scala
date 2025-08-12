package transformations

import java.util.{List => JList}
import com.analytics.framework.core.base.{TransformModule, PipelineCtx}

/** โดเมนยังไม่มีกติกาเฉพาะ ให้ผ่านค่าไปก่อน */
class MemberAnalyticsTransform extends TransformModule[Map[String,Any], Map[String,Any]] {
  override def transform(records: JList[Map[String,Any]], params: Map[String,Any])
                        (implicit ctx: PipelineCtx): JList[Map[String,Any]] = records
}
