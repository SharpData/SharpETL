package com.github.sharpdata.sharpetl.spark.utils

import org.apache.http.StatusLine

object HttpStatusUtils {

  def isSuccessful(statusLine: StatusLine): Boolean = {
    statusLine.getStatusCode/100 == 2
  }
}
