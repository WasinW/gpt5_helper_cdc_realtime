package com.analytics.framework.utils
import com.google.cloud.secretmanager.v1.{SecretManagerServiceClient, AccessSecretVersionRequest}
object SecretManagerUtil {
  def access(projectId:String, secretName:String, version:String="latest"): String = {
    val client = SecretManagerServiceClient.create()
    try {
      val name = s"projects/$projectId/secrets/$secretName/versions/$version"
      val req = AccessSecretVersionRequest.newBuilder().setName(name).build()
      val resp = client.accessSecretVersion(req)
      new String(resp.getPayload.getData.toByteArray(), java.nio.charset.StandardCharsets.UTF_8)
    } finally client.close()
  }
}
