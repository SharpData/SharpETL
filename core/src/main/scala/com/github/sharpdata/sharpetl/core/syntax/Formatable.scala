package com.github.sharpdata.sharpetl.core.syntax

import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.annotation.Annotations.Stable
import org.apache.commons.lang3.reflect.FieldUtils

import scala.collection.mutable
import scala.jdk.CollectionConverters._

@Stable(since = "1.0.0")
class Formatable extends Serializable {

  override def toString: String = format(this)

  // scalastyle:off
  final def format(obj: Any): String = {
    obj match {
      case value: String => value
      case _ =>
        FieldUtils
          .getAllFieldsList(obj.getClass)
          .asScala
          .filterNot(it => it.getName.contains("$init$") || it.getName.contains("$outer") || it.getName.contains("$jacoco"))
          .map(field => {
            field.setAccessible(true)
            val value = field.get(this)
            value match {
              case null => ""
              case _: String => s"-- ${field.getName}=$value"
              case _: Integer => s"-- ${field.getName}=$value"
              case args: mutable.Map[String, Any] =>
                if (args.nonEmpty) {
                  (List("-- args") ++ args.map { case (key, value) => s"--  $key=$value" }).mkString("\n")
                } else ""
              case options: Map[String, String] =>
                if (options.nonEmpty) {
                  (List("-- options") ++ options.map { case (key, value) => s"--  $key=$value" }).mkString("\n")
                } else ""
              case _ => s"-- ${field.getName}\n" + value.toString.replaceAll("-- ", "--  ")
            }
          }).filterNot(isNullOrEmpty).mkString("\n")
    }
  }
  // scalastyle:on
}
