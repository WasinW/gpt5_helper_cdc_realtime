package com.analytics.framework.utils
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets

object GcsFileIO {
  private def ensureFS(): Unit = {
    if (FileSystems.getDefaultPipelineOptions() == null) {
      val opts: PipelineOptions = PipelineOptionsFactory.create()
      FileSystems.setDefaultPipelineOptions(opts)
    }
  }
  def readText(path: String): String = {
    if (!path.startsWith("gs://")) {
      return new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path)), StandardCharsets.UTF_8)
    }
    ensureFS()
    val res = FileSystems.matchNewResource(path, false)
    val in = Channels.newInputStream(FileSystems.open(res))
    try new String(in.readAllBytes(), StandardCharsets.UTF_8) finally in.close()
  }
}
