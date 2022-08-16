package com.github.sharpdata.sharpetl.core.util

import com.github.sharpdata.sharpetl.core.util.Constants.Environment
import com.github.sharpdata.sharpetl.core.exception.Exception.MissingConfigurationException

import java.io.File
import java.math.BigInteger
import java.util.UUID

object StringUtil {
  def assertNotEmpty(value: String, name: String): Unit = {
    if (isNullOrEmpty(value)) {
      throw new IllegalArgumentException(s"$name can not be null or empty")
    }
  }

  def environmentSuffix: String = {
    Option(Environment.CURRENT).getOrElse(Environment.LOCAL).toLowerCase match {
      case "" => ""
      case Environment.LOCAL => ""
      case Environment.DEV => s"-${Environment.DEV}"
      case Environment.QA => s"-${Environment.QA}"
      case Environment.PROD => s"-${Environment.PROD}"
      case Environment.TEST => s"-${Environment.TEST}"
      case _ => s"-${Environment.CURRENT}"
    }
  }

  val EMPTY: String = ""

  def canNotBeEmpty(name: String, value: String): (String, String) = {
    if (isNullOrEmpty(value)) {
      throw MissingConfigurationException(name)
    } else {
      (name, value)
    }
  }

  def getParentPath(path: String): String = {
    path.substring(0, path.lastIndexOf(File.separator))
  }

  def getFileNameFromPath(path: String): String = {
    path.substring(path.lastIndexOf(File.separator) + 1)
  }

  def concatFilePath(dir: String, fileName: String): String = {
    s"$dir${
      if (dir.endsWith(File.separator)) {
        ""
      } else {
        File.separator
      }
    }$fileName"
  }

  def getPrefix(prefix: String): String = {
    Option(prefix).filterNot(isNullOrEmpty).map(_.concat(".")).getOrElse("")
  }

  def uuid: String = {
    UUID.randomUUID().toString.replace("-", "")
  }

  def uuidName(): String = {
    UUID.randomUUID().toString.split('-').head
  }

  def uuidName(prefix: String, function: String): String = {
    prefix
      .replace("\"", "")
      .replace("`", "")
      .replace(" ", "_")
      .replace(".", "_").split("__")(0) + "__" + function + "__" + UUID.randomUUID().toString.split('-').head
  }

  def getTempName(prefix: String, function: String): String = {
    prefix
      .replace("\"", "")
      .replace("`", "")
      .replace(" ", "_")
      .replace(".", "_").split("__")(0) + "__" + function
  }

  def humpToUnderline(hump: String): String = {
    val builder = new StringBuilder
    hump.foreach(ch => {
      if (ch.isUpper) {
        builder.append("_")
      }
      builder.append(ch.toLower)
    })
    builder.toString()
  }

  def isNullOrEmpty(x: String): Boolean = {
    x == null || x.isEmpty
  }

  def humpToUnderlineWithUpperCaseCheck(hump: String): String = {
    val builder = new StringBuilder
    var humpCheck = hump
    if (humpCheck == humpCheck.toUpperCase) {
      humpCheck = humpCheck.toLowerCase
    }

    var upperIndex = 0
    humpCheck.zipWithIndex.foreach(ch => {
      if (ch._1.isUpper && ch._2 != upperIndex + 1 && ch._2 != 0) {
        upperIndex = ch._2
        builder.append("_")
      }
      builder.append(ch._1.toLower)
    })
    builder.toString()
  }

  implicit class BigIntConverter(value: String) {
    def asBigInt: BigInteger = new BigInteger(value)
  }

}
