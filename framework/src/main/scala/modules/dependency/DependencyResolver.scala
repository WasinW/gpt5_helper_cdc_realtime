package com.analytics.framework.modules.dependency

/**
  * Resolves dependencies defined in configuration into concrete
  * actions.  For instance, if table A depends on table B being
  * complete, this class would query an external metadata store
  * (e.g. BigQuery) to verify that B has finished before A begins.
  * Not used directly in this stub but provided for completeness.
  */
class DependencyResolver {
  def resolve(dependencyConfig: Map[String, Any]): Boolean = {
    println("DependencyResolver: resolving dependencies")
    true
  }
}