package com.analytics.framework.connectors.s3

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.core.JsonFactory
// import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.util.Try

// AWS SDK v2
import software.amazon.awssdk.auth.credentials.{DefaultCredentialsProvider, AwsCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, ListObjectsV2Request}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.core.ResponseInputStream

object S3JsonlReader {
  // private val om = new ObjectMapper(new JsonFactory()).registerModule(DefaultScalaModule)
  private val om = new ObjectMapper(new JsonFactory())


  private def toDate(windowId:String): String = {
    // Accept yyyyMMddHHmm, yyyyMMddHH, yyyyMMdd; slice safely
    val digits = windowId.takeWhile(_.isDigit)
    if (digits.length >= 12) digits.substring(0,12)
    else if (digits.length >= 10) digits.substring(0,10)
    else digits
  }

  private case class S3Url(bucket:String, keyPrefix:String)
  private def parseS3Uri(uri:String): S3Url = {
    // s3://bucket/prefix/...
    val noScheme = uri.stripPrefix("s3://")
    val p = noScheme.indexOf('/')
    if (p < 0) S3Url(noScheme, "")
    else S3Url(noScheme.substring(0,p), noScheme.substring(p+1))
  }

  def readJsonlForTable(projectId:String, cfg: java.util.Map[String,AnyRef], zone:String, table:String, windowId:String): List[Map[String,Any]] = {
    val conf = cfg.asScala.toMap
    // Allow override via mock local directory for tests
    conf.get("mock_s3_dir") match {
      case Some(dirAny) =>
        val dir = String.valueOf(dirAny)
        val sub = java.nio.file.Paths.get(dir, zone, table, toDate(windowId))
        if (!java.nio.file.Files.exists(sub)) return Nil
        val files = java.nio.file.Files.list(sub).iterator().asScala.toList
        files.flatMap { p =>
          val br = java.nio.file.Files.newBufferedReader(p, StandardCharsets.UTF_8)
          try readJsonLines(br) finally br.close()
        }
      case None =>
        (for {
          urlAny <- conf.get("aws_snapshot_s3").toList
        } yield {
          val base = String.valueOf(urlAny)
          val basePath = if (base.endsWith("/")) base.dropRight(1) else base
          val prefixUri = s"""$basePath/$zone/$table/${toDate(windowId)}/"""
          readFromS3(prefixUri)
        }).flatten
    }
  }

  private def readJsonLines(br: BufferedReader): List[Map[String,Any]] = {
    val out = scala.collection.mutable.ListBuffer.empty[Map[String,Any]]
    var line: String = null
    while ({ line = br.readLine(); line != null }) {
      val t = line.trim
      if (t.nonEmpty) {
        Try(om.readTree(t)).toOption.foreach { node =>
          out += nodeToScala(node)
        }
      }
    }
    out.toList
  }

  private def nodeToScala(n: JsonNode): Map[String,Any] = {
    import scala.collection.mutable
    val m = mutable.Map.empty[String,Any]
    val it = n.fields()
    while (it.hasNext) {
      val e = it.next()
      val key = e.getKey
      val v = e.getValue
      val scalaV: Any =
        if (v.isNull) null
        else if (v.isTextual) v.asText()
        else if (v.isNumber) v.numberValue()
        else if (v.isBoolean) java.lang.Boolean.valueOf(v.asBoolean())
        else if (v.isArray) v.elements().asScala.map(nodeToScalaAny).toList.asJava // keep lists as java for BQ later
        else if (v.isObject) nodeToScala(v)
        else v.asText()
      m += key -> scalaV
    }
    m.toMap
  }

  private def nodeToScalaAny(n: JsonNode): Any =
    if (n.isNull) null
    else if (n.isTextual) n.asText()
    else if (n.isNumber) n.numberValue()
    else if (n.isBoolean) java.lang.Boolean.valueOf(n.asBoolean())
    else if (n.isObject) nodeToScala(n)
    else if (n.isArray) n.elements().asScala.map(nodeToScalaAny).toList.asJava
    else n.asText()

  private def readFromS3(prefixUri:String): List[Map[String,Any]] = {
    val s3Url = parseS3Uri(prefixUri)
    val region = sys.env.get("AWS_REGION").map(Region.of).getOrElse(Region.AP_SOUTHEAST_1)
    val s3 = S3Client.builder()
      .credentialsProvider(DefaultCredentialsProvider.create())
      .region(region)
      .build()
    try {
      val req = ListObjectsV2Request.builder()
        .bucket(s3Url.bucket)
        .prefix(s3Url.keyPrefix)
        .build()
      val listing = s3.listObjectsV2(req)
      val keys = listing.contents().asScala.map(_.key()).filter(_.endsWith(".jsonl")).toList
      keys.flatMap { key =>
        val gor = GetObjectRequest.builder().bucket(s3Url.bucket).key(key).build()
        val in: ResponseInputStream[_] = s3.getObject(gor)
        val br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
        try readJsonLines(br) finally br.close()
      }
    } finally {
      Try(s3.close())
    }
  }
}
