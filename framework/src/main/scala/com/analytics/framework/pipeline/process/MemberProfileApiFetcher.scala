package com.analytics.framework.pipeline.process
import org.apache.beam.sdk.transforms.DoFn
import com.analytics.framework.pipeline.model.{Notification, CDCRecord}
import com.analytics.framework.connectors.SecretManager
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import java.time.Duration
import com.google.gson.{Gson, JsonParser}
import scala.jdk.CollectionConverters._
class MemberProfileApiFetcher(projectId: String, baseUrl: String, secretId: String, timeoutSec: Int = 10, rateLimitRps: Int = 30)
  extends DoFn[Notification, CDCRecord] {
  @transient private var http: HttpClient = _
  @DoFn.Setup def setup(): Unit = { http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(timeoutSec.toLong)).build() }
  @DoFn.ProcessElement
  def process(ctx: DoFn[Notification, CDCRecord]#ProcessContext): Unit = {
    val n = ctx.element(); if (n.accountId == null || n.accountId.isEmpty) return
    val token = SecretManager.access(projectId, secretId, "latest").trim
    val url = s"$baseUrl/accounts/${n.accountId}?profileRole=OWNER&pageNumber=1&pageSize=10"
    val req = HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(timeoutSec)).header("Authorization", s"Bearer $token").header("Accept", "application/json").GET().build()
    val res = http.send(req, HttpResponse.BodyHandlers.ofString())
    if (res.statusCode() >= 200 && res.statusCode() < 300) {
      val body = res.body()
      val parser = new JsonParser(); val je = parser.parse(body)
      val map = new java.util.HashMap[String, AnyRef]()
      val obj = if (je.isJsonArray) je.getAsJsonArray.get(0).getAsJsonObject else je.getAsJsonObject
      for (e <- obj.entrySet().asScala) map.put(e.getKey, if (e.getValue.isJsonNull) null else e.getValue.getAsString)
      ctx.output(CDCRecord(n.accountId, "upsert", map, "s_loy_program", "raw"))
    } else {
      // TODO: DLQ
    }
  }
}
