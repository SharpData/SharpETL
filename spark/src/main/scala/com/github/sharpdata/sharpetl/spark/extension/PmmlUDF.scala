package com.github.sharpdata.sharpetl.spark.extension

import com.github.sharpdata.sharpetl.core.util.{ETLLogger, IOUtil, StringUtil}

import java.io.FileInputStream
import java.util
import org.apache.spark.sql.Row
import org.dmg.pmml.FieldName
import org.jpmml.evaluator.{Evaluator, EvaluatorUtil, FieldValue, InputField, LoadingModelEvaluatorBuilder}

import scala.jdk.CollectionConverters._

// $COVERAGE-OFF$
class PmmlUDF extends Serializable {
  private val pmmlFileRootDir = "pmml"

  private var evaluator: Evaluator = _

  def apply(pmmlFileName: String): Unit = {
    evaluator = initEvaluator(pmmlFileName)
  }

  private def initEvaluator(pmmlFileName: String): Evaluator = {
    val pmmlPath = IOUtil
      .recursiveListFilesFromResource(pmmlFileRootDir)
      .find(path => pmmlFileName == StringUtil.getFileNameFromPath(path))
    val pmmlIs = if (pmmlPath.isDefined) {
      new FileInputStream(pmmlPath.get)
    }  else {
      IOUtil.getInputStreamFromJar(pmmlFileName)
    }
    val evaluator: Evaluator = new LoadingModelEvaluatorBuilder()
      .load(pmmlIs)
      .build()
    evaluator.verify()
    ETLLogger.info(String.format(
      "PMML input fields: \n%s",
      evaluator.getInputFields.asScala.mkString("\n\t")
    ))
    ETLLogger.info(String.format(
      "PMML target fields: \n%s",
      evaluator.getTargetFields.asScala.mkString("\n\t")
    ))
    pmmlIs.close()
    evaluator
  }

  private def rowToArgumentsMapper(inputFields: util.List[InputField]): Row => util.Map[FieldName, FieldValue] = {
    val mappers = inputFields.asScala.map(inputField => {
      (row: Row, arguments: util.Map[FieldName, FieldValue]) =>
        arguments.put(inputField.getName, inputField.prepare(row.get(row.fieldIndex(inputField.getName.getValue))))
    })

    row: Row => {
      val arguments = new util.HashMap[FieldName, FieldValue]()
      mappers.foreach(mapper => {
        mapper(row, arguments)
      })
      arguments
    }
  }

  def predict(row: Row): Map[String, String] = {
    val inputFields = evaluator.getInputFields
    val arguments = rowToArgumentsMapper(inputFields)(row)
    val results: util.Map[FieldName, _] = evaluator.evaluate(arguments)
    results
      .asScala
      .map {
        case (targetFieldName, targetFieldValue) =>
          targetFieldName.getValue -> EvaluatorUtil.decode(targetFieldValue).toString
      }
      .toMap
  }

}

// $COVERAGE-ON$
