package com.analytics.framework.core.base
case class PipelineCtx(projectId:String, region:String, domain:String,
  datasets:Map[String,String], buckets:Map[String,String], windowPattern:String,
  configRoot:String, options:Map[String,Any]=Map.empty) extends Serializable
