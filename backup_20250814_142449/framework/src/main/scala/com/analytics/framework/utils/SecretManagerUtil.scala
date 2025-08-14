package com.analytics.framework.utils
object SecretManagerUtil{
  def access(name:String): String =
    sys.env.getOrElse(name.replaceAll(".*/", ""), "")
}
