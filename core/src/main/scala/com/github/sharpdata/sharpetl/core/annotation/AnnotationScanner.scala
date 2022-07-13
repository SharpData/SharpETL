package com.github.sharpdata.sharpetl.core.annotation

import com.github.sharpdata.sharpetl.core.datasource.config.{DBDataSourceConfig, DataSourceConf, DataSourceConfig}
import com.github.sharpdata.sharpetl.core.annotation.Annotations.Private
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}
import io.github.classgraph.ClassGraph

import scala.jdk.CollectionConverters._

@Private
object AnnotationScanner {

  val defaultConfigType: Class[DataSourceConfig] = classOf[DBDataSourceConfig].asInstanceOf[Class[DataSourceConfig]]

  val configRegister: Map[String, Class[DataSourceConfig]] =
    new ClassGraph()
      .acceptPackages("com.github.sharpdata.sharpetl")
      .enableClassInfo()
      .enableAnnotationInfo()
      .scan()
      .getClassesImplementing(classOf[DataSourceConf])
      .getNames
      .asScala
      .map(clazz => {
        try {
          val `class` = Class.forName(clazz)
          val annotation = `class`.getAnnotation(classOf[configFor])
          if (annotation == null) {
            None
          } else {
            val sourceTypes = annotation.types()
            Some(`class`.asInstanceOf[Class[DataSourceConfig]], sourceTypes)
          }
        } catch {
          case _: Throwable => None
        }
      })
      .filter(_.nonEmpty)
      .map(_.get.swap)
      .flatMap(it => it._1.map {
        `type` => (`type`, it._2)
      })
      .toMap

  val sourceRegister: Map[String, Class[Source[_, _]]] = {
    new ClassGraph()
      .acceptPackages("com.github.sharpdata.sharpetl")
      .enableClassInfo()
      .enableAnnotationInfo()
      .scan()
      .getClassesImplementing(classOf[Source[_, _]])
      .getNames
      .asScala
      .map(clazz => {
        try {
          val `class` = Class.forName(clazz)
          val annotation = `class`.getAnnotation(classOf[source])
          if (annotation == null) {
            None
          } else {
            val sourceTypes = annotation.types()
            Some(`class`.asInstanceOf[Class[Source[_, _]]], sourceTypes)
          }
        } catch {
          case _: Throwable => None
        }
      })
      .filter(_.nonEmpty)
      .map(_.get.swap)
      .flatMap(it => it._1.map {
        `type` => (`type`, it._2)
      })
      .toMap
  }

  val sinkRegister: Map[String, Class[Sink[_]]] =
    new ClassGraph()
      .acceptPackages("com.github.sharpdata.sharpetl")
      .enableClassInfo()
      .enableAnnotationInfo()
      .scan()
      .getClassesImplementing(classOf[Sink[_]])
      .getNames
      .asScala
      .map(clazz => {
        try {
          val `class` = Class.forName(clazz)
          val annotation = `class`.getAnnotation(classOf[sink])
          if (annotation == null) {
            None
          } else {
            val sourceTypes = annotation.types()
            Some(`class`.asInstanceOf[Class[Sink[_]]], sourceTypes)
          }
        } catch {
          case _: Throwable => None
        }
      })
      .filter(_.nonEmpty)
      .map(_.get.swap)
      .flatMap(it => it._1.map {
        `type` => (`type`, it._2)
      })
      .toMap

}
