package com.github.sharpdata.sharpetl.spark.utils

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.spark.transformation.Transformer
import com.github.sharpdata.sharpetl.core.util.WorkflowReader.readLines
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.TransformerType
import com.github.sharpdata.sharpetl.core.util.ReflectUtil.reflectObjectMethod
import com.github.sharpdata.sharpetl.core.util.ScalaScriptCompiler
import org.apache.spark.sql.DataFrame

import scala.language.reflectiveCalls

object ReflectUtil {

  def reflectClassMethod(className: String, args: Any*): Any = {
    val c = Class.forName(className)
      .newInstance
      // scalastyle:off
      .asInstanceOf[ {def entrypoint(config: Any): DataFrame}]
    // scalastyle:on
    c.entrypoint(args)
  }

  def compileAndCallObjectMethod(className: String, methodName: String, args: Any*): Any = {
    val objectName = className.substring(className.lastIndexOf(".") + 1)
    val transformer = ScalaScriptCompiler.compileTransformer(readLines(objectName).mkString("\n"))
      .asInstanceOf[Transformer]
    if (args.size == 1) {
      transformer.transform(args.head.asInstanceOf[Map[String, String]])
    } else {
      transformer.transform(args(0).asInstanceOf[DataFrame], args(1).asInstanceOf[WorkflowStep], args(2).asInstanceOf[Variables])
    }
  }

  def execute(className: String,
              methodName: String,
              transformerType: String,
              args: Any*): Any = {
    transformerType match {
      case TransformerType.OBJECT_TYPE => reflectObjectMethod(className, methodName, args: _*)
      case TransformerType.CLASS_TYPE => reflectClassMethod(className, args: _*)
      case TransformerType.DYNAMIC_OBJECT_TYPE => compileAndCallObjectMethod(className, methodName, args: _*)
      case _ => throw new RuntimeException(s"Unknown transformer type: $transformerType")
    }
  }
}
