package com.analytics.framework.connectors.s3
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, ListObjectsV2Request}
import java.io.{BufferedReader, InputStreamReader}
import scala.collection.mutable.ListBuffer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.analytics.framework.utils.SecretManagerUtil
import com.analytics.framework.utils.JsonUtils // optional if available; else use mapper directly
import scala.jdk.CollectionConverters._
import com.analytics.framework.utils.YamlLoader

object S3JsonlReader {
  private val om = new ObjectMapper().registerModule(DefaultScalaModule)

  case class AwsCreds(id:String, secret:String, region:String)
  private def loadCreds(project:String, cfg: Map[String,Any]): AwsCreds = {
    val s3 = cfg("s3").asInstanceOf[Map[String,Any]]
    val region = s3.getOrElse("region","ap-southeast-1").toString
    val creds = s3.getOrElse("creds", Map.empty).asInstanceOf[Map[String,Any]]
    creds.get("source").map(_.toString).getOrElse("env") match {
      case "gcp_secret_manager" =>
        val sec = creds.getOrElse("secret_name","aws_ee_default").toString
        val raw = SecretManagerUtil.access(project, sec, "latest")
        val js  = om.readTree(raw)
        AwsCreds(
          id     = js.get("AWS_ACCESS_KEY_ID").asText(),
          secret = js.get("AWS_SECRET_ACCESS_KEY").asText(),
          region = Option(js.get("AWS_REGION")).map(_.asText()).getOrElse(region)
        )
      case _ =>
        AwsCreds(
          id     = sys.env.getOrElse("AWS_ACCESS_KEY_ID",""),
          secret = sys.env.getOrElse("AWS_SECRET_ACCESS_KEY",""),
          region = sys.env.getOrElse("AWS_REGION", region)
        )
    }
  }

  private def client(creds: AwsCreds): S3Client = {
    val prov = StaticCredentialsProvider.create(AwsBasicCredentials.create(creds.id, creds.secret))
    S3Client.builder().region(Region.of(creds.region)).credentialsProvider(prov).build()
  }

  /** Read JSONL objects under s3://bucket/prefix/dt=YYYY-MM-DD/ filtering by table name.
    * It matches keys in these forms:
    *   .../table=<table>/part-*.jsonl
    *   .../<table>*.jsonl
    */
  def readJsonlForTable(projectId:String, cfg: Map[String,Any], zone:String, table:String, windowId:String): Iterable[Map[String,Any]] = {
    val s3c = cfg("s3").asInstanceOf[Map[String,Any]]
    val bucket = s3c.getOrElse("bucket","eagleeye").toString
    val base   = s3c.getOrElse("base_prefix","member").toString
    val dt     = toDate(windowId) // 20250810T0100 -> 2025-08-10
    val prefix = s"$base/$zone/dt=$dt/"
    val creds  = loadCreds(projectId, cfg); val cli = client(creds)

    val req = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build()
    val it  = cli.listObjectsV2Paginator(req).iterator()
    val keys = ListBuffer[String]()
    while(it.hasNext){
      val page = it.next()
      page.contents().forEach(obj => {
        val k = obj.key()
        if (k.endsWith(".jsonl") && (k.contains(s"/table=$table/") || k.split("/").last.startsWith(table))) keys += k
      })
    }
    val rows = ListBuffer[Map[String,Any]]()
    keys.foreach { k =>
      val goreq = GetObjectRequest.builder().bucket(bucket).key(k).build()
      val is = cli.getObject(goreq)
      val br = new BufferedReader(new InputStreamReader(is, java.nio.charset.StandardCharsets.UTF_8))
      try {
        var line: String = null
        while({ line = br.readLine(); line != null }) {
          if (line.trim.nonEmpty) {
            val node = om.readTree(line)
            val m: Map[String,Any] = om.convertValue(node, classOf[Map[String,Object]]).asInstanceOf[Map[String,Any]]
            rows += m
          }
        }
      } finally { br.close(); is.close() }
    }
    rows.toList
  }

  private def toDate(windowId:String): String = {
    // assumes yyyyMMdd... -> yyyy-MM-dd
    if (windowId.length>=8) s"${windowId[0:4]}-${windowId[4:6]}-${windowId[6:8]}" else windowId
  }
}
