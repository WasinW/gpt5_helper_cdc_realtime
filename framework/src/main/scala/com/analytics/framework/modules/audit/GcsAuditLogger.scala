package com.analytics.framework.modules.audit
import java.io.{File, FileWriter, BufferedWriter}

object GcsAuditLogger {
  def writeLine(path: String, line: String): Unit = {
    if (path.startsWith("gs://")) {
      println(s"[AUDIT] $path :: $line") // local dev: log to stdout
    } else {
      val f = new File(path)
      f.getParentFile.mkdirs()
      val bw = new BufferedWriter(new FileWriter(f, true))
      try { bw.write(line); bw.newLine() } finally bw.close()
    }
  }
}
