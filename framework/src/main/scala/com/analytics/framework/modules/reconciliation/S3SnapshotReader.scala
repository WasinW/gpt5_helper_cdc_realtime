package com.analytics.framework.modules.reconciliation

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import scala.jdk.CollectionConverters._
import java.nio.charset.StandardCharsets

class S3SnapshotReader(bucket: String, prefix: String) {
  @transient private var s3: S3Client = _

  def init(): Unit = { s3 = S3Client.create() }

  def listKeys(limit: Int = 64): List[String] = {
    val req = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).maxKeys(limit).build()
    s3.listObjectsV2(req).contents().asScala.map(_.key()).toList
  }

  def readObjectLines(key: String): Iterator[String] = {
    val get = GetObjectRequest.builder().bucket(bucket).key(key).build()
    val in = s3.getObject(get)
    scala.io.Source.fromInputStream(in, StandardCharsets.UTF_8.name()).getLines()
  }
}