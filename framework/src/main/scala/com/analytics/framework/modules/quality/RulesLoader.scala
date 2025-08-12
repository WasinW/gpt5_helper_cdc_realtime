package com.analytics.framework.modules.quality
import com.analytics.framework.utils.YamlLoader
import scala.collection.JavaConverters._
object RulesLoader{
  def loadNotNullRules(path:String): List[String] = {
    val m = YamlLoader.load(path)
    m.get("not_null") match {
      case Some(l: java.util.List[_]) => l.asScala.toList.map(_.toString)
      case Some(l: List[_])           => l.map(_.toString)
      case _                          => Nil
    }
  }
}
