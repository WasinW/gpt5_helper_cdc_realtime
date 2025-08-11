package com.analytics.framework.modules.audit
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.CreateOptions
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.io.fs.ResourceIdCoder
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets

object GcsAuditLogger {
  def writeLine(path: String, line: String): Unit = {
    val rid = FileSystems.matchNewResource(path, false)
    val out = Channels.newOutputStream(FileSystems.create(rid, CreateOptions.StandardCreateOptions.builder().build()))
    try out.write((line + "\n").getBytes(StandardCharsets.UTF_8))
    finally out.close()
  }
}
