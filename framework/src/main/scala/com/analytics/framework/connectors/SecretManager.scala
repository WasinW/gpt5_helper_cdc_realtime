package com.analytics.framework.connectors
import com.google.cloud.secretmanager.v1.{AccessSecretVersionRequest, SecretManagerServiceClient}
object SecretManager {
  def access(projectId: String, secretId: String, version: String = "latest"): String = {
    val name = s"projects/$projectId/secrets/$secretId/versions/$version"
    val client = SecretManagerServiceClient.create()
    try {
      val req = AccessSecretVersionRequest.newBuilder().setName(name).build()
      val res = client.accessSecretVersion(req)
      new String(res.getPayload.getData.toByteArray, java.nio.charset.StandardCharsets.UTF_8)
    } finally client.close()
  }
}
