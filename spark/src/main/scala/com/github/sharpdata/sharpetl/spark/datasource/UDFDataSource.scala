package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.config.{ClassDataSourceConfig, PmmlDataSourceConfig, UDFDataSourceConfig}
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.spark.extension.UDFExtension
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.runtime.universe.{MethodSymbol, TermName}
import scala.reflect.runtime.{universe => ru}

case class ClassInfo(
                      instanceMirror: ru.InstanceMirror,
                      defaultMethodSymbol: ru.MethodSymbol,
                      methodSymbols: Map[String, ru.MethodSymbol]) {
  def invoke(args: Any*): Any = {
    instanceMirror.reflectMethod(defaultMethodSymbol)(args)
  }
}

@source(types = Array("class", "object", "pmml"))
@sink(types = Array("udf"))
class UDFConfigExtension extends Source[DataFrame, SparkSession] with Sink[DataFrame] {
  private val METHOD_NAME_APPLY = "apply"
  @transient private val classMirror: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)
  @transient private val classInfoMap: collection.mutable.Map[String, ClassInfo] =
    collection.mutable.Map[String, ClassInfo]()

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    apply(executionContext, step.getSourceConfig)
  }

  override def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = registerUDF(df.sparkSession, step)

  def apply(spark: SparkSession, sourceConfig: ClassDataSourceConfig): DataFrame = {
    val args = getClassArgs(sourceConfig)
    apply(
      sourceConfig.getDataSourceType,
      sourceConfig.getClassName,
      args: _*
    )
    spark.emptyDataFrame
  }

  def apply(classType: String, className: String, args: Any*): ClassInfo = this.synchronized {
    val classInfoKey = (classType +: (className +: args)).mkString
    if (!classInfoMap.isDefinedAt(classInfoKey)) {
      val classInfo = classType.toLowerCase match {
        case DataSourceType.CLASS | DataSourceType.PMML =>
          reflectClass(className, args: _*)
        case DataSourceType.OBJECT =>
          reflectObject(className)
      }
      classInfoMap += classInfoKey -> classInfo
      ETLLogger.info(s"Dynamic load class: $classInfo")
    }
    classInfoMap(classInfoKey)
  }

  private def buildClassInfo(
                              instanceMirror: ru.InstanceMirror,
                              methodSymbols: Iterable[MethodSymbol]): ClassInfo = {
    val defaultMethodSymbol = methodSymbols.head
    val methodSymbolMap = methodSymbols
      .map(symbol => symbol.name.toString -> symbol)
      .toMap
    ClassInfo(
      instanceMirror,
      defaultMethodSymbol,
      methodSymbolMap
    )
  }

  private def reflectClass(className: String, args: Any*): ClassInfo = {
    val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = runtimeMirror.staticClass(className).asClass
    val methodSymbols = classSymbol
      .typeSignature
      .members
      .filter(_.isMethod)
      .map(_.asMethod)
    val constructors = methodSymbols.filter(_.isConstructor)
    val constructorMirror = runtimeMirror
      .reflectClass(classSymbol)
      .reflectConstructor(constructors.head.asMethod)
    val instance = constructorMirror()
    val instanceMirror = runtimeMirror.reflect(instance)
    val applyMethodSymbol = methodSymbols
      .find(symbol => symbol.name == TermName(METHOD_NAME_APPLY) &&
        symbol.paramLists.head.size == args.size)
    if (applyMethodSymbol.isDefined) {
      instanceMirror.reflectMethod(applyMethodSymbol.get)(args: _*)
    }
    buildClassInfo(
      instanceMirror,
      methodSymbols
    )
  }

  private def reflectObject(className: String): ClassInfo = {
    val moduleSymbol = classMirror.staticModule(className)
    val moduleMirror = classMirror.reflectModule(moduleSymbol)
    val instanceMirror = classMirror.reflect(moduleMirror.instance)
    val methodSymbols = moduleSymbol
      .typeSignature
      .members
      .filter(_.isMethod)
      .map(_.asMethod)
    buildClassInfo(
      instanceMirror,
      methodSymbols
    )
  }

  def getClassArgs(sourceConfig: ClassDataSourceConfig): Seq[String] = {
    sourceConfig match {
      case config: PmmlDataSourceConfig =>
        Seq(config.getPmmlFileName)
      case _ =>
        Nil
    }
  }

  def registerUDF(spark: SparkSession, step: WorkflowStep): Unit = {
    val sourceConfig = step.getSourceConfig[ClassDataSourceConfig]
    val targetConfig = step.getTargetConfig[UDFDataSourceConfig]
    val args = getClassArgs(sourceConfig)
    UDFExtension.registerUDF(
      spark,
      sourceConfig.getDataSourceType,
      targetConfig.getUdfName,
      sourceConfig.getClassName,
      targetConfig.getMethodName,
      args: _*
    )
  }

  def generateFunction(
                        classType: String,
                        className: String,
                        methodName: Option[String],
                        argumentsNum: Int,
                        classArgs: Any*): AnyRef = {
    lazy val udfClassInfo = this.apply(classType, className, classArgs: _*)
    lazy val instanceMirror = udfClassInfo.instanceMirror
    lazy val methodSymbol = udfClassInfo.methodSymbols(methodName.getOrElse(METHOD_NAME_APPLY))
    argumentsNum match {
      case 0 => new (() => Any) with Serializable {
        override def apply(): Any = {
          try {
            instanceMirror.reflectMethod(methodSymbol)()
          } catch {
            case e: Exception =>
              ETLLogger.error(e.getMessage)
              throw e
          }
        }
      }
      case 1 => new (Any => Any) with Serializable {
        override def apply(v1: Any): Any = {
          try {
            instanceMirror.reflectMethod(methodSymbol)(v1)
          } catch {
            case e: Exception =>
              ETLLogger.error(e.getMessage)
              throw e
          }
        }
      }
      case 2 => new ((Any, Any) => Any) with Serializable {
        override def apply(v1: Any, v2: Any): Any = {
          try {
            instanceMirror.reflectMethod(methodSymbol)(v1, v2)
          } catch {
            case e: Exception =>
              ETLLogger.error(e.getMessage)
              throw e
          }
        }
      }
      case 3 => new ((Any, Any, Any) => Any) with Serializable {
        override def apply(v1: Any, v2: Any, v3: Any): Any = {
          try {
            instanceMirror.reflectMethod(methodSymbol)(v1, v2, v3)
          } catch {
            case e: Exception =>
              ETLLogger.error(e.getMessage)
              throw e
          }
        }
      }
    }
  }
}

object UDFConfigExtension extends UDFConfigExtension
