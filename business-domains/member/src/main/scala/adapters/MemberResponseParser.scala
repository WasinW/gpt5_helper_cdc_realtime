package com.domain.member.adapters
import com.analytics.framework.connectors.http.ApiResponse
import com.analytics.framework.utils.JsonUtils
object MemberResponseParser {
  def toMaps(resp: ApiResponse): List[Map[String,Any]] = List(JsonUtils.toMap(resp.body))
}
