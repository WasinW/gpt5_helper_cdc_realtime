package com.analytics.framework.utils

import java.security.MessageDigest

object HashUtils {
  def md5(s: String): String = {
    val md = MessageDigest.getInstance("MD5")
    md.update(s.getBytes("UTF-8"))
    md.digest.map("%02x".format(_)).mkString
  }
}