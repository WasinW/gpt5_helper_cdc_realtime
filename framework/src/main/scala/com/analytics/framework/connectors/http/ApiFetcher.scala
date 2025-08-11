package com.analytics.framework.connectors.http
final case class ApiRequest(method:String, url:String, headers:Map[String,String]=Map.empty,
  query:Map[String,String]=Map.empty, body:Option[String]=None, timeoutSec:Int=10, targetTable:String="")
final case class ApiResponse(status:Int, body:String, headers:Map[String,String]=Map.empty)
trait ApiFetcher extends Serializable { def execute(req: ApiRequest): ApiResponse }
