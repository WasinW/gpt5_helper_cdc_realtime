package com.analytics.framework.connectors
object SecretManager {
  def getSecret(name: String): String = sys.env.getOrElse(name, "")
}
