package com.analytics.framework.modules.reconciliation
import com.analytics.framework.utils.YamlLoader

object ReconcileMappingLoader {
  case class TableMap(gcsToS3: Map[String,String])
  case class ZoneMaps(tables: Map[String, TableMap])
  case class All(zone: Map[String, ZoneMaps])

  def load(path: String): Map[String, Map[String, Map[String,String]]] = {
    // returns: zone -> table -> gcs_to_s3 map
    val cfg = YamlLoader.load(path)
    val rec = cfg.getOrElse("reconcile", Map.empty).asInstanceOf[Map[String,Any]]
    val mapping = rec.getOrElse("mapping", Map.empty).asInstanceOf[Map[String,Any]]
    mapping.map { case (zone, v) =>
      val tables = v.asInstanceOf[Map[String,Any]].map { case (tbl, tv) =>
        val g2s = tv.asInstanceOf[Map[String,Any]].getOrElse("gcs_to_s3", Map.empty).asInstanceOf[Map[String,String]]
        tbl -> g2s
      }
      zone -> tables
    }
  }

  def get(path:String, zone:String, table:String): Map[String,String] =
    load(path).getOrElse(zone, Map.empty).getOrElse(table, Map.empty)
}
