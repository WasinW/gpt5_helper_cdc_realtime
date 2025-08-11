package com.domain.member.adapters
import com.analytics.framework.connectors.http.ApiRequest
final case class Notification(accountId:String, eventType:String)
final case class MemberApiCfg(baseUrl:String, token:String, profileRole:String, timeout:Int)
object MemberRequestBuilder {
  def fromNotification(api: MemberApiCfg)(evt: Notification): ApiRequest = {
    val url = s"${api.baseUrl.stripSuffix("/")}/accounts/${evt.accountId}"
    ApiRequest(method="GET", url=url,
      headers=Map("Authorization" -> s"Bearer ${api.token}"),
      query=Map("profileRole"->api.profileRole,"pageNumber"->"1","pageSize"->"10"),
      timeoutSec=api.timeout, targetTable="s_loy_program")
  }
}
