package com.analytics.framework.modules.reconciliation
class S3SampleProvider {
  def sample(projectId:String,
             cfg: java.util.Map[String,AnyRef],
             zone:String,
             table:String,
             windowId:String,
             limit:Int = 10): List[Map[String,Any]] = {
    // stub: ยังไม่อ่าน S3 จริง แค่ให้ compile ผ่าน
    Nil
  }
}
