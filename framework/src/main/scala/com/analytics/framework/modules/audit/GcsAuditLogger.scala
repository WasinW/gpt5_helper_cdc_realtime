package com.analytics.framework.modules.audit
object GcsAuditLogger{
  def writeLine(path:String, line:String): Unit =
    System.out.println(s"[AUDIT] $path :: $line")
}
