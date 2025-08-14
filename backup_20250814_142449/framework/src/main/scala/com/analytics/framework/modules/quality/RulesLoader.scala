package com.analytics.framework.modules.quality
import com.analytics.framework.utils.YamlLoader

object RulesLoader {
  type Rule[T] = T => List[String] // empty = pass

  def loadNotNullRules(path: String): List[Rule[Map[String, Any]]] = {
    val cfg = YamlLoader.load(path)
    val fields: List[String] = cfg.get("not_null") match {
      case Some(xs: Iterable[_]) => xs.collect { case s: String => s }.toList
      case Some(other)           => throw new IllegalArgumentException(s"not_null must be list of strings, got $other")
      case None                  => Nil
    }
    fields.map { f =>
      (m: Map[String, Any]) =>
        val ok = m.get(f).exists(v => v != null && v.toString.trim.nonEmpty)
        if (ok) Nil else List(s"NOT_NULL_FAIL:$f")
    }
  }
}
