package com.analytics.framework.connectors

/**
  * Simplified Pub/Sub connector.  Provides helper methods to read
  * messages with attributes from subscriptions.  In a real
  * implementation this would call into Beam's `PubsubIO`.
  */
class PubSubConnector(projectId: String) {
  def read(subscription: String): Any = {
    println(s"PubSubConnector: reading from subscription $subscription")
    new Object()
  }
}