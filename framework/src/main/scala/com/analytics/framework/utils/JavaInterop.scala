package com.analytics.framework.utils
import scala.collection.JavaConverters._

object JavaInterop {
  def deepAsScala(v: Any): Any = v match {
    case m: java.util.Map[_, _] =>
      m.asScala.iterator.map { case (k, v2) => String.valueOf(k) -> deepAsScala(v2) }.toMap
    case l: java.util.List[_]   => l.asScala.map(deepAsScala).toList
    case s: java.util.Set[_]    => s.asScala.map(deepAsScala).toSet
    case other                  => other
  }
  def deepMap(v: Any): Map[String, Any] =
    deepAsScala(v).asInstanceOf[Map[String, Any]]
}
