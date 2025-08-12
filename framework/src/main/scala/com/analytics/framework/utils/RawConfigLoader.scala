package com.analytics.framework.utils

object RawConfigLoader {

  def load(configPath: String): RawIngestConfig = {
    // YamlLoader.load คืนค่า Map[String, Any] (Scala)
    val cfg: Map[String, Any] = YamlLoader.load(configPath)

    // หยิบส่วน raw_ingest ออกมาเป็น Map[String, Any]
    val raw: Map[String, Any] = cfg.get("raw_ingest") match {
      case Some(m: Map[_, _])            => m.asInstanceOf[Map[String, Any]]
      case Some(jm: java.util.Map[_, _]) =>
        import scala.collection.JavaConverters._
        jm.asInstanceOf[java.util.Map[String, Any]].asScala.toMap
      case _                             => Map.empty[String, Any]
    }

    def asOptString(x: Any): Option[String] = x match {
      case null         => None
      case s: String    => Some(s)
      case other        => Some(other.toString)
    }

    val idField: Option[String]    = raw.get("id_field").flatMap(asOptString)
    val tableField: Option[String] = raw.get("table_field").flatMap(asOptString)

    val fields: List[(String, String)] = raw.get("fields") match {
      case Some(m: Map[_, _]) =>
        m.asInstanceOf[Map[String, Any]].toList.map { case (k, v) => k.toString -> v.toString }
      case Some(jm: java.util.Map[_, _]) =>
        import scala.collection.JavaConverters._
        jm.asInstanceOf[java.util.Map[String, Any]].asScala.toList.map { case (k, v) => k.toString -> v.toString }
      // เผื่อ config บางแบบเขียนเป็น list ของ 1-entry map
      case Some(it: Iterable[_]) =>
        it.toList.collect {
          case m: Map[_, _] if m.size == 1 =>
            val (k, v) = m.head; k.toString -> v.toString
          case (k: String, v) =>
            k -> v.toString
        }
      case _ => Nil
    }

    RawIngestConfig(idField, tableField, fields)
  }
}
