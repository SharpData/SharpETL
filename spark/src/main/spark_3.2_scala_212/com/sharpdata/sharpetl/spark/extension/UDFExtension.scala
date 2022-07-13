package com.github.sharpdata.sharpetl.spark.extension

import com.github.sharpdata.sharpetl.spark.datasource.UDFConfigExtension
import com.github.sharpdata.sharpetl.spark.datasource.UDFConfigExtension.generateFunction
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, ScalaReflection}
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.util.Try

object UDFExtension {

  def registerUDF(
                   spark: SparkSession,
                   classType: String,
                   name: String,
                   className: String,
                   methodName: String,
                   args: Any*): Unit = {
    val (fun, inputEncoders, returnType) = getFunctionInfo(
      classType,
      className,
      methodName,
      args: _*
    )

    def builder(e: Seq[Expression]) = ScalaUDF(
      function = fun,
      dataType = returnType,
      children = e,
      inputEncoders = if (inputEncoders.contains(None)) Nil else inputEncoders,
      udfName = Some(name)
    )

    val functionIdentifier = new FunctionIdentifier(name)
    spark.sessionState.functionRegistry.registerFunction(functionIdentifier, builder _, "scala_udf")
  }

  def getFunctionInfo(
                       classType: String,
                       className: String,
                       methodName: String,
                       args: Any*): (AnyRef, Seq[Option[ExpressionEncoder[_]]], DataType) = {
    val classInfo = UDFConfigExtension.apply(classType, className, args: _*)
    val methodSymbol = classInfo.methodSymbols(methodName)
    // 此处反射生成 UDF 时不考虑柯里化
    val params = methodSymbol
      .paramLists
      .head
    val parameterTypes = params
      .map(param => Try(ScalaReflection.schemaFor(param.typeSignature).dataType).toOption)

    val schema = StructType(params.map(param => {
      StructField(param.fullName, ScalaReflection.schemaFor(param.typeSignature).dataType)
    }))

    //TODO: 可能不工作
    val inputEncoders = Seq(Option(RowEncoder.apply(schema).resolveAndBind()))

    val returnType = ScalaReflection.schemaFor(methodSymbol.returnType).dataType
    val fun = generateFunction(
      classType,
      className,
      Some(methodName),
      parameterTypes.length,
      args: _*
    )
    (fun, Nil, returnType)
  }


}
