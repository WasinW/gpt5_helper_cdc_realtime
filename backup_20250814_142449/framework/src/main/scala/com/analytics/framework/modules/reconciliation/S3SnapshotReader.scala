package com.analytics.framework.modules.reconciliation
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{ListObjectsV2Request, GetObjectRequest}
import scala.collection.JavaConverters._
import java.nio.charset.StandardCharsets
import java.io.BufferedReader
import java.io.InputStreamReader
class S3SnapshotReader(s3: S3Client){
  def listKeys(bucket:String, prefix:String): List[String] = {
    val req = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build()
    s3.listObjectsV2(req).contents().asScala.map(_.key()).toList
  }
  def readLines(bucket:String, key:String): Iterator[String] = {
    val req = GetObjectRequest.builder().bucket(bucket).key(key).build()
    val is  = s3.getObject(req)
    val br  = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
    Iterator.continually(br.readLine()).takeWhile(_ != null)
  }
}
