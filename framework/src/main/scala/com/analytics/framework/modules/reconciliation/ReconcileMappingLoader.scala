package com.analytics.framework.modules.reconciliation
import com.analytics.framework.utils.YamlLoader
import scala.collection.JavaConverters._
object ReconcileMappingLoader{
  def load(path:String): Map[String, Map[String, Map[String,String]]] = {
    val root = YamlLoader.load(path)
    // รองรับทั้งรูปแบบที่ wrap ใน key: mapping หรือ map ตรงๆ
    val raw = root.get("mapping").getOrElse(root)
    raw match {
      case m: java.util.Map[_,_] =>
        m.asInstanceOf[java.util.Map[String, java.util.Map[String, java.util.Map[String,String]]]]
         .asScala.map{ case (zone, mtables) =>
            zone -> mtables.asScala.map{ case (tbl, kv) => tbl -> kv.asScala.toMap }.toMap
         }.toMap
      case m: Map[_,_] =>
        m.asInstanceOf[Map[String, Map[String, Map[String,String]]]]
      case _ => Map.empty
    }
  }
  def get(path:String, zone:String, table:String): Map[String,String] =
    load(path).getOrElse(zone, Map.empty).getOrElse(table, Map.empty)
}
