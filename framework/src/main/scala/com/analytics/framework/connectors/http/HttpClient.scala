package com.analytics.framework.connectors.http
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import java.time.Duration
class HttpClient extends ApiFetcher {
  @transient private lazy val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build()
  def execute(req: ApiRequest): ApiResponse = {
    val uri = if (req.query.isEmpty) URI.create(req.url)
      else URI.create(req.url + "?" + req.query.map{case(k,v)=> s"$k=$v"}.mkString("&"))
    val b = HttpRequest.newBuilder().uri(uri).timeout(Duration.ofSeconds(req.timeoutSec.toLong))
    req.headers.foreach{ case(k,v)=> b.header(k,v) }
    val httpReq = req.method.toUpperCase match {
      case "GET"  => b.GET().build()
      case "POST" => b.POST(HttpRequest.BodyPublishers.ofString(req.body.getOrElse(""))).build()
      case _      => b.GET().build()
    }
    val res = client.send(httpReq, HttpResponse.BodyHandlers.ofString())
    ApiResponse(res.statusCode(), res.body(), Map.empty)
  }
}
