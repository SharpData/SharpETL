package com.github.sharpdata.sharpetl.flink.quality

import com.github.sharpdata.sharpetl.core.annotation.Annotations.Stable
import com.github.sharpdata.sharpetl.core.quality.QualityCheck._
import com.github.sharpdata.sharpetl.core.quality.{DataQualityCheckResult, DataQualityConfig, ErrorType, QualityCheck, QualityCheckRule}
import com.github.sharpdata.sharpetl.core.repository.QualityCheckAccessor
import com.github.sharpdata.sharpetl.core.util.{ETLLogger, StringUtil}
import com.github.sharpdata.sharpetl.flink.extra.driver.FlinkJdbcStatement.fixedResult
import com.github.sharpdata.sharpetl.flink.job.Types.DataFrame
import org.apache.flink.table.api.Expressions._
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.{TableEnvironment, ValidationException}
import org.apache.flink.table.operations.{ModifyOperation, Operation, QueryOperation}

import java.util
import java.util.List
import scala.jdk.CollectionConverters.asScalaIteratorConverter

@Stable(since = "1.0.0")
class FlinkQualityCheck(val tEnv: TableEnvironment,
                        override val dataQualityCheckRules: Map[String, QualityCheckRule],
                        override val qualityCheckAccessor: QualityCheckAccessor)
  extends QualityCheck[DataFrame] {

  override def queryCheckResult(sql: String): Seq[DataQualityCheckResult] = {
    if (sql.trim == "") {
      Seq()
    } else {
      ETLLogger.info(s"execution sql:\n $sql")
      tEnv.sqlQuery(sql).execute().collect().asScala
        .map(it => DataQualityCheckResult(
          it.getField(0).toString, // column
          it.getField(1).toString, // dataCheckType
          it.getField(2).toString, // ids
          it.getField(3).toString.split(DELIMITER).head, // errorType
          it.getField(4).toString.toInt, // warnCount
          it.getField(5).toString.toInt) // errorCount
        )
        .filterNot(it => it.warnCount < 1 && it.errorCount < 1)
        .toSeq
    }
  }

  override def execute(sql: String): DataFrame = {
    ETLLogger.info(s"Execution sql: \n $sql")
    val impl = tEnv.asInstanceOf[TableEnvironmentImpl]
    val operations: util.List[Operation] = impl.getParser.parse(sql)
    if (operations.size != 1) {
      throw new ValidationException("Unsupported SQL query! sqlQuery() only accepts a single SQL query.")
    }
    else {
      val operation: Operation = operations.get(0)
      operation match {
        case op: QueryOperation if !operation.isInstanceOf[ModifyOperation] =>
          impl.createTable(op)
        case _ =>
          tEnv.executeSql(sql)
          fixedResult
      }
    }
  }

  override def createView(df: DataFrame, tempViewName: String): Unit = {
    ETLLogger.info(s"Creating temp view `$tempViewName`")
    tEnv.createTemporaryView(s"`$tempViewName`", df)
  }

  override def dropView(tempViewName: String): Unit = {
    ETLLogger.info(s"Dropping temp view `$tempViewName`")
    tEnv.dropTemporaryView(s"`$tempViewName`")
  }

  override def dropUnusedCols(df: DataFrame, cols: String): DataFrame = {
    df.dropColumns(cols.split(",").map(col => $(col.trim)).toArray: _*)
  }

  override def windowByPkSql(tempViewName: String, idColumns: String, sortColumns: String = "", desc: Boolean = true): String = {
    s"""
       |SELECT *, 1 as __row_num
       |FROM (SELECT *, MAX($sortColumns)
       |      OVER (PARTITION BY $idColumns) as __max__
       |      FROM `$tempViewName`
       |) WHERE $idColumns = __max__""".stripMargin
  }

  override def windowByPkSqlErrors(tempViewName: String, idColumns: String, sortColumns: String = "", desc: Boolean = true): String = {
    s"""
       |SELECT ${joinIdColumns(idColumns)} as id,
       |        ARRAY['Duplicated PK check$DELIMITER$idColumns'] as error_result,
       |        1 as __row_num
       |FROM (SELECT *, MAX($sortColumns)
       |      OVER (PARTITION BY $idColumns) as __max__
       |      FROM `$tempViewName`
       |) WHERE $idColumns = __max__""".stripMargin
  }

  override def generateErrorUnions(dataQualityCheckMapping: Seq[DataQualityConfig], topN: Int, view: String): String = {
    dataQualityCheckMapping
      .filter(_.errorType == ErrorType.error)
      .map(it =>
        s"""(SELECT
           |    '${it.column}' as `column`,
           |    '${it.dataCheckType}' as dataCheckType,
           |    LISTAGG(CAST(id as STRING)) as ids,
           |    '${it.errorType}' as errorType,
           |    0 as warnCount,
           |    count(*) as errorCount
           |FROM `$view`
           |WHERE ARRAY_CONTAINS(error_result, '${it.dataCheckType}${DELIMITER}${it.column}')
           |)""".stripMargin
      )
      .mkString("\nUNION ALL\n")
  }

  override def generateWarnUnions(dataQualityCheckMapping: Seq[DataQualityConfig], topN: Int, view: String): String = {
    dataQualityCheckMapping
      .filter(_.errorType == ErrorType.warn)
      .map(it =>
        s"""(SELECT
           |    '${it.column}' as `column`,
           |    '${it.dataCheckType}' as dataCheckType,
           |    LISTAGG(CAST(id as STRING)) as ids,
           |    '${it.errorType}' as errorType,
           |    count(*) as warnCount,
           |    0 as errorCount
           |FROM `$view`
           |WHERE ARRAY_CONTAINS(warn_result, '${it.dataCheckType}${DELIMITER}${it.column}')
           |)""".stripMargin
      )
      .mkString("\nUNION ALL\n")
  }

  override def checkSql(tempViewName: String, resultView: String, dataQualityCheckMapping: Seq[DataQualityConfig], idColumn: String): String = {
    s"""
       |CREATE TEMPORARY VIEW `$resultView`
       |AS SELECT ${joinIdColumns(idColumn)} as id,
       |       ARRAY[${generateWarnCases(dataQualityCheckMapping)}
       |             ] as warn_result,
       |       ARRAY[${generateErrorCases(dataQualityCheckMapping)}
       |             ] as error_result
       |FROM `$tempViewName`
      """.stripMargin
  }

  override def udrWarnSql(topN: Int, udrWithViews: Seq[(DataQualityConfig, String)])
  : String = {
    if (udrWithViews.isEmpty) {
      StringUtil.EMPTY
    } else {
      udrWithViews.map { case (udr, viewName) =>
          s"""
             |(SELECT '${udr.column}' as column,
             |    '${udr.dataCheckType}' as dataCheckType,
             |    LISTAGG(CAST(id as STRING)) as ids,
             |    '${udr.errorType}' as errorType,
             |    count(*) as warnCount,
             |    0 as errorCount
             |FROM `$viewName`)
             |""".stripMargin
        }
        .mkString("\nUNION ALL\n")
    }
  }

  override def udrErrorSql(topN: Int, udrWithViews: Seq[(DataQualityConfig, String)])
  : String = {
    if (udrWithViews.isEmpty) {
      StringUtil.EMPTY
    } else {
      udrWithViews.map { case (udr, viewName) =>
          s"""
             |(SELECT '${udr.column}' as column,
             |    '${udr.dataCheckType}' as dataCheckType,
             |    LISTAGG(CAST(id as STRING)) as ids,
             |    '${udr.errorType}' as errorType,
             |    0 as warnCount,
             |    count(*) as errorCount
             |FROM `$viewName`)
             |""".stripMargin
        }
        .mkString("\nUNION ALL\n")
    }
  }

  override def antiJoinSql(idColumn: String, tempViewName: String, resultView: String): String = {
    s"""|WHERE `$tempViewName`.`$idColumn` NOT IN (
        |  SELECT id FROM `$resultView`
        |       WHERE CARDINALITY(error_result) > 0
        |)
      """.stripMargin
  }

  override def udrAntiJoinSql(idColumn: String, tempViewName: String, viewNames: Seq[String]): String = {
    if (viewNames.isEmpty) {
      StringUtil.EMPTY
    } else {
      s"""|WHERE `$tempViewName`.`$idColumn` NOT IN (
          | SELECT id FROM (${viewNames.map(view => s"SELECT id FROM $view").mkString("\nUNION ALL\n")})
          |      WHERE CARDINALITY(error_result) > 0
          |)
          |""".stripMargin
    }
  }

  def emptyArrayIfMissing(query: String): String = {
    if (query.trim == "") {
      "array[]"
    } else {
      query
    }
  }

  def generateWarnCases(dataQualityCheckMapping: Seq[DataQualityConfig]): String = {
    emptyArrayIfMissing(dataQualityCheckMapping
      .filter(_.errorType == ErrorType.warn)
      .map(it => s"""CASE WHEN ${it.rule} THEN '${it.dataCheckType}${DELIMITER}${it.column}' ELSE '' END""")
      .mkString(",\n\t\t\t\t")
    )
  }

  def generateErrorCases(dataQualityCheckMapping: Seq[DataQualityConfig]): String = {
    emptyArrayIfMissing(dataQualityCheckMapping
      .filter(_.errorType == ErrorType.error)
      .map(it => s"""CASE WHEN ${it.rule} THEN '${it.dataCheckType}${DELIMITER}${it.column}' ELSE '' END""")
      .mkString(",\n\t\t\t\t")
    )
  }
}
