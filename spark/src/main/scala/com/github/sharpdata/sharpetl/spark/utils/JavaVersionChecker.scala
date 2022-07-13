package com.github.sharpdata.sharpetl.spark.utils

import ETLSparkSession.sparkSession
import com.github.sharpdata.sharpetl.core.util.ETLLogger

import scala.util.control.NoStackTrace

object JavaVersionChecker {

  final case class InvalidJavaVersionException(message: String) extends RuntimeException(message) with NoStackTrace

  def checkJavaVersion(): Unit = {
    val version = System.getProperty("java.version")

    version.substring(0, version.indexOf(".")) match {
      case "1" => if (!version.startsWith("1.8")) {
        throw InvalidJavaVersionException(s"Java $version lower than 1.8 is not supported")
      }
      case "9" | "10" => throw InvalidJavaVersionException(s"Non LTS java $version may not supported")
      case "11" => if (sparkSession.version.startsWith("2.")) {
        throw InvalidJavaVersionException(s"Currently using java $version, which may not supported by Apache Spark(<3.0)")
      }
      case _ => ETLLogger.error(s"Java $version higher than 11 may not supported!")
    }
  }
}
