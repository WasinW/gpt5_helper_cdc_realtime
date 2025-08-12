package com.analytics.framework.utils

object RawConfigLoader {
  def load(path: String): RawIngestConfig = {
    val cfg: Map[String,Any] = YamlLoader.load(path)
    val root: Map[String,Any] =
      cfg.get("raw_ingest").collect{ case m: Map[_,_] => m.asInstanceOf[Map[String,Any]] }.getOrElse(Map.empty)

    val idField    = root.get("id_field").map(_.toString)
    val tableField = root.get("table_field").map(_.toString)
    val fieldsList: List[(String,String)] =
      root.get("fields") match {
        case Some(l: List[_]) =>
          l.collect {
            case m: Map[_, _] =>
              val mm = m.asInstanceOf[Map[String,Any]]
              mm.getOrElse("name","").toString -> mm.getOrElse("spec","").toString
          }
        case _ => Nil
      }

    RawIngestConfig(idField, tableField, fieldsList)
  }
}
