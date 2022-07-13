package com.github.sharpdata.sharpetl.core.util

import scala.reflect.runtime
import scala.tools.reflect.{FrontEnd, ToolBox}

object ScalaScriptCompiler {

  final case class DynamicCompileFailedException(message: String) extends RuntimeException(message)

  private val toolbox = ToolBox(runtime.currentMirror).mkToolBox(
    frontEnd = new FrontEnd {
      override def display(info: Info): Unit = {
        info.severity.toString() match {
          case "ERROR" => ETLLogger.error(
            s"""
               |Compile ${info.severity}: ${info.msg}
               |near:
               |${info.pos.source.lineToString(info.pos.line - 1)}
               |${info.pos.source.lineToString(info.pos.line)}
               |
               |Helping message:
               |
               |NOTE: You could not using `foreachRDD`, `foreachPartition` or any operator that will involve multiple nodes.
               |
               |If you encounter error "illegal cyclic reference involving object InterfaceAudience",
               |That because you are using some API related to `org.apache.hadoop.fs.Path`,
               |which triggered a bug of Scala: https://github.com/scala/bug/issues/12190,
               |you will need spark-submit options `--conf  "spark.executor.userClassPathFirst=true" --conf  "spark.driver.userClassPathFirst=true"`
               |(NOTE: this options ONLY works in yarn cluster mode)
               |
               |If you encounter error "object x is not a member of package x",
               |please don't use import but use full package name, like `scala.collection.mutable.Map[String, String]`,
               |sometimes inline the reference works as well.
               |""".stripMargin)
          case _ => ETLLogger.warn(info.toString)
        }
      }

      def interactive(): Unit = ()
    },
    options = ""
  )

  private def doCompileTransformer(code: String): Any = {
    if (code.startsWith("package ")) {
      throw DynamicCompileFailedException("`package` should not used in scala script.")
    }
    toolbox.eval(toolbox.parse(
      s"""
         |$code
         |
         |${extractObjectName(code)}
         |""".stripMargin))
  }

  private def extractObjectName(code: String) = {
    val objectKeywordIndex = code.indexOf("object")
    val extendsKeywordIndex = code.indexOf("extends")
    code.slice(objectKeywordIndex + 6, extendsKeywordIndex).trim
  }

  val compileTransformer: Memo1[String, Any] = Memo1 { (code: String) => doCompileTransformer(code) }
}
