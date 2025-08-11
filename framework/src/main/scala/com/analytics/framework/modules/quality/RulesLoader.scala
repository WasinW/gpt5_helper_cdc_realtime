package com.analytics.framework.modules.quality
import com.analytics.framework.core.base.{QualityRule, PipelineCtx}
import com.analytics.framework.utils.YamlLoader

object RulesLoader {
  def loadNotNullRules(path: String): List[QualityRule[Map[String,Any]]] = {
    val cfg = YamlLoader.load(path)
    val rules = cfg.get("rules").collect{ case l: List[Map[String, @unchecked]] => l }.getOrElse(Nil)
    rules.flatMap { r =>
      (for {
        t <- r.get("type").collect{ case s: String => s }
        cols <- r.get("columns").collect{ case l: List[_] => l.collect{ case s: String => s } }
        if t.equalsIgnoreCase("not_null")
      } yield cols.map(new NotNullRule(_))).getOrElse(Nil)
    }
  }
}
