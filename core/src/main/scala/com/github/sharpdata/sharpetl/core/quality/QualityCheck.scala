package com.github.sharpdata.sharpetl.core.quality

import com.github.sharpdata.sharpetl.core.annotation.Annotations.Stable
import com.github.sharpdata.sharpetl.core.exception.Exception.DataQualityCheckRuleMissingException
import com.github.sharpdata.sharpetl.core.quality.QualityCheck._
import com.github.sharpdata.sharpetl.core.repository.QualityCheckAccessor
import com.github.sharpdata.sharpetl.core.repository.model.QualityCheckLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.ReflectUtil.reflectObjectMethod
import com.github.sharpdata.sharpetl.core.util.{ETLLogger, StringUtil}
import com.github.sharpdata.sharpetl.core.util.StringUtil.uuidName
import com.google.common.base.Strings.isNullOrEmpty

// scalastyle:off
case class CheckResult[DataFrame](warn: Seq[DataQualityCheckResult], error: Seq[DataQualityCheckResult], passed: DataFrame)

final case class CheckStep[DataFrame](warn: Seq[DataQualityCheckResult], error: Seq[DataQualityCheckResult], sql: String) {
  def union(other: CheckStep[DataFrame], passed: DataFrame): CheckResult[DataFrame] = {
    CheckResult(warn ++ other.warn, error ++ other.error, passed)
  }

  def union(others: Seq[CheckStep[DataFrame]], passed: DataFrame): CheckResult[DataFrame] = {
    val warns = others.map(_.warn).fold(warn)(_ ++ _)
    val errors = others.map(_.error).fold(error)(_ ++ _)
    CheckResult(warns, errors, passed)
  }
}


@Stable(since = "1.0.0")
trait QualityCheck[DataFrame] extends Serializable {
  val dataQualityCheckRules: Map[String, QualityCheckRule]
  val qualityCheckAccessor: QualityCheckAccessor

  def qualityCheck(step: WorkflowStep, jobId: String, jobScheduleId: String,
                   df: DataFrame): CheckResult[DataFrame] = {
    val idColumn = step.source.options.getOrElse("idColumn", "id")
    val sortColumn = step.source.options.getOrElse("sortColumn", "")
    val desc = step.source.options.getOrElse("desc", "true")
    val dataQualityConfigs = parseQualityConfig(step)
    val configs = dataQualityConfigs.groupBy(_.rule.startsWith(UserDefinedRule.PREFIX))

    val udrConfigs = configs.getOrElse(true, Seq())
    val sqlConfigs = configs.getOrElse(false, Seq())

    val tempViewName = uuidName()
    val preWindowedViewName = tempViewName + "_pre_windowed"
    createView(df, preWindowedViewName)
    val distinctDf = dropUnusedCols(execute(windowByPkSql(preWindowedViewName, idColumn, sortColumn, desc.toBoolean)), "__row_num")
    createView(distinctDf, tempViewName)

    val topN = step.source.options.getOrElse("topN", DEFAULT_TOP_N.toString).toInt

    val checkDuplicateStep = checkDuplicate(preWindowedViewName, idColumn, sortColumn, desc.toBoolean, topN)

    val sqlStep: CheckStep[DataFrame] =
      if (sqlConfigs.isEmpty) {
        CheckStep(Seq(), Seq(), StringUtil.EMPTY)
      } else {
        check(tempViewName, sqlConfigs, idColumn, topN)
      }

    val (udrStep: CheckStep[DataFrame], views: Seq[String]) =
      if (udrConfigs.isEmpty) {
        (CheckStep(Seq(), Seq(), StringUtil.EMPTY), List())
      } else {
        checkUDR(tempViewName, udrConfigs, idColumn, topN)
      }

    val resultView = s"${tempViewName}_result"
    val passed: DataFrame = execute(generateAntiJoinSql(sqlStep.sql, udrStep.sql, tempViewName))

    val result = sqlStep.union(Seq(checkDuplicateStep, udrStep), passed)

    ETLLogger.warn(s"Found ${result.warn.size} warn(s) in job $jobScheduleId")
    ETLLogger.error(s"Found ${result.error.size} error(s) in job $jobScheduleId")
    recordCheckResult(jobId, jobScheduleId, result.error ++ result.warn ++ checkDuplicateStep.error)
    dropView(tempViewName)
    dropView(resultView)
    dropView(preWindowedViewName)
    views.foreach(dropView)
    result
  }

  def parseQualityConfig(step: WorkflowStep): Seq[DataQualityConfig] =
    step.source.options
      .filter { case (k, _) => k.startsWith("column") }
      .flatMap { case (key, rules) =>
        val columnName = key.split("\\.").toList.tail.head
        rules.split(",").map { ruleType =>
          if (!dataQualityCheckRules.contains(ruleType.trim)) {
            val msg = s"rule type: ${ruleType.trim} is missing from config file quality-check.yaml"
            ETLLogger.error(msg)
            throw DataQualityCheckRuleMissingException(msg)
          }
          dataQualityCheckRules(ruleType.trim).withColumn(columnName)
        }.toSeq
      }
      .toSeq


  def recordCheckResult(jobId: String, jobScheduleId: String, results: Seq[DataQualityCheckResult]): Unit = {
    results
      .filter(it => it.warnCount > 0 || it.errorCount > 0)
      .map(it =>
        QualityCheckLog(jobId, jobScheduleId, it.column, it.dataCheckType, it.ids, it.errorType, it.warnCount, it.errorCount)
      ).foreach(qualityCheckAccessor.create)
  }

  def checkDuplicate(tempViewName: String, idColumn: String,
                     sortColumns: String, desc: Boolean, topN: Int = DEFAULT_TOP_N): CheckStep[DataFrame] = {
    //    val windowSql = windowByPkSql(tempViewName, idColumn, sortColumns, desc)
    //    val distinctDf = dropUnusedCols(execute(windowSql), "__row_num")
    //    createView(distinctDf, tempViewName)

    val duplicatedViewName = s"${tempViewName}_duplicated"
    val duplicatedDf = execute(windowByPkSqlErrors(tempViewName, idColumn, sortColumns, desc))
    createView(duplicatedDf, duplicatedViewName)

    val configs = Seq(DataQualityConfig(idColumn, "Duplicated PK check", "", ErrorType.error))
    val errors: Seq[DataQualityCheckResult] = queryCheckResult(generateErrorUnions(configs, topN, duplicatedViewName))

    CheckStep(Seq(), errors, "") // "" because there are no data will be filtered here.
  }

  def check(tempViewName: String, dataQualityCheckMapping: Seq[DataQualityConfig],
            idColumn: String, topN: Int = DEFAULT_TOP_N): CheckStep[DataFrame] = {
    val resultView = s"${tempViewName}_result"
    val resultSql = checkSql(tempViewName, resultView, dataQualityCheckMapping, idColumn)
    execute(resultSql)
    val warns: Seq[DataQualityCheckResult] = queryCheckResult(generateWarnUnions(dataQualityCheckMapping, topN, resultView))
    val errors: Seq[DataQualityCheckResult] = queryCheckResult(generateErrorUnions(dataQualityCheckMapping, topN, resultView))

    CheckStep(warns, errors, antiJoinSql(idColumn, tempViewName, resultView))
  }

  def checkUDR(tempViewName: String, dataQualityCheckMapping: Seq[DataQualityConfig],
               idColumn: String, topN: Int = DEFAULT_TOP_N): (CheckStep[DataFrame], Seq[String]) = {
    val udrWithViews = dataQualityCheckMapping.map { udr =>
      val className = udr.rule.replace(s"${UserDefinedRule.PREFIX}.", "")
      val (sql, viewName) = reflectObjectMethod(className, "check", tempViewName, idColumn, udr).asInstanceOf[(String, String)]
      execute(sql)
      (udr, viewName)
    }
    val warns: Seq[DataQualityCheckResult] = queryCheckResult(udrWarnSql(topN, udrWithViews.filter(_._1.errorType == "warn")))
    val errors: Seq[DataQualityCheckResult] = queryCheckResult(udrErrorSql(topN, udrWithViews.filter(_._1.errorType == "error")))

    val antiJoinSql = udrAntiJoinSql(idColumn, tempViewName, udrWithViews.filter(_._1.errorType == "error").map(_._2))

    (CheckStep(warns, errors, antiJoinSql), udrWithViews.map(_._2))
  }

  def queryCheckResult(sql: String): Seq[DataQualityCheckResult]

  def execute(sql: String): DataFrame

  def createView(df: DataFrame, tempViewName: String): Unit

  def dropView(tempViewName: String): Unit

  def dropUnusedCols(df: DataFrame, cols: String): DataFrame

  def windowByPkSql(tempViewName: String, idColumns: String, sortColumns: String = "", desc: Boolean = true): String = {
    s"""
       |SELECT *
       |FROM (SELECT *, ROW_NUMBER()
       |      OVER (PARTITION BY $idColumns
       |      ORDER BY ${if (isNullOrEmpty(sortColumns)) "1" else sortColumns} ${if (desc) "DESC" else "ASC"}) as __row_num
       |      FROM $tempViewName
       |) WHERE __row_num = 1""".stripMargin
  }


  def windowByPkSqlErrors(tempViewName: String, idColumns: String, sortColumns: String = "", desc: Boolean = true): String = {
    s"""
       |SELECT ${joinIdColumns(idColumns)} as id,
       |        ARRAY('Duplicated PK check$DELIMITER$idColumns') as error_result
       |FROM (SELECT *, ROW_NUMBER()
       |      OVER (PARTITION BY $idColumns
       |      ORDER BY ${if (isNullOrEmpty(sortColumns)) "1" else sortColumns} ${if (desc) "DESC" else "ASC"}) as __row_num
       |      FROM $tempViewName
       |) WHERE __row_num > 1""".stripMargin
  }

  def generateErrorUnions(dataQualityCheckMapping: Seq[DataQualityConfig], topN: Int, view: String): String = {
    dataQualityCheckMapping
      .filter(_.errorType == ErrorType.error)
      .map(it =>
        s"""(SELECT
           |    "${it.column}" as column,
           |    "${it.dataCheckType}" as dataCheckType,
           |    arrayJoin(top(collect_list(string(id)), $topN), ',') as ids,
           |    "${it.errorType}" as errorType,
           |    0 as warnCount,
           |    count(*) as errorCount
           |FROM `$view`
           |WHERE array_contains(error_result, "${it.dataCheckType}${DELIMITER}${it.column}")
           |)""".stripMargin
      )
      .mkString("\nUNION ALL\n")
  }

  def generateWarnUnions(dataQualityCheckMapping: Seq[DataQualityConfig], topN: Int, view: String): String = {
    dataQualityCheckMapping
      .filter(_.errorType == ErrorType.warn)
      .map(it =>
        s"""(SELECT
           |    "${it.column}" as column,
           |    "${it.dataCheckType}" as dataCheckType,
           |    arrayJoin(top(collect_list(string(id)), $topN), ',') as ids,
           |    "${it.errorType}" as errorType,
           |    count(*) as warnCount,
           |    0 as errorCount
           |FROM `$view`
           |WHERE array_contains(warn_result, "${it.dataCheckType}${DELIMITER}${it.column}")
           |)""".stripMargin
      )
      .mkString("\nUNION ALL\n")
  }

  def checkSql(tempViewName: String, resultView: String, dataQualityCheckMapping: Seq[DataQualityConfig], idColumn: String): String = {
    s"""
       |CREATE TEMPORARY VIEW $resultView
       |AS SELECT ${joinIdColumns(idColumn)} as id,
       |       flatten(ARRAY(${generateWarnCases(dataQualityCheckMapping)}
       |             )) as warn_result,
       |       flatten(ARRAY(${generateErrorCases(dataQualityCheckMapping)}
       |             )) as error_result
       |FROM `$tempViewName`
      """.stripMargin
  }

  def udrWarnSql(topN: Int, udrWithViews: Seq[(DataQualityConfig, String)])
  : String = {
    if (udrWithViews.isEmpty) {
      StringUtil.EMPTY
    } else {
      udrWithViews.map { case (udr, viewName) =>
          s"""
             |(SELECT "${udr.column}" as column,
             |    "${udr.dataCheckType}" as dataCheckType,
             |    arrayJoin(top(collect_list(string(id)), $topN), ',') as ids,
             |    "${udr.errorType}" as errorType,
             |    count(*) as warnCount,
             |    0 as errorCount
             |FROM $viewName)
             |""".stripMargin
        }
        .mkString("\nUNION ALL\n")
    }
  }

  def udrErrorSql(topN: Int, udrWithViews: Seq[(DataQualityConfig, String)])
  : String = {
    if (udrWithViews.isEmpty) {
      StringUtil.EMPTY
    } else {
      udrWithViews.map { case (udr, viewName) =>
          s"""
             |(SELECT "${udr.column}" as column,
             |    "${udr.dataCheckType}" as dataCheckType,
             |    arrayJoin(top(collect_list(string(id)), $topN), ',') as ids,
             |    "${udr.errorType}" as errorType,
             |    0 as warnCount,
             |    count(*) as errorCount
             |FROM $viewName)
             |""".stripMargin
        }
        .mkString("\nUNION ALL\n")
    }
  }


  def antiJoinSql(idColumn: String, tempViewName: String, resultView: String): String = {
    s"""|LEFT ANTI JOIN (
        |  SELECT id FROM `$resultView`
        |       WHERE size(error_result) > 0
        |) bad_ids ON bad_ids.id = ${joinIdColumns(idColumn, tempViewName)}
      """.stripMargin
  }

  def udrAntiJoinSql(idColumn: String, tempViewName: String, viewNames: Seq[String]): String = {
    if (viewNames.isEmpty) {
      StringUtil.EMPTY
    } else {
      s"""|LEFT ANTI JOIN (
          |${viewNames.map(view => s"SELECT id FROM $view").mkString("\nUNION ALL\n")}
          |) udr_bad_ids ON udr_bad_ids.id = ${joinIdColumns(idColumn, tempViewName)}
          |""".stripMargin
    }
  }
}
// scalastyle:on

object QualityCheck {
  val DELIMITER = "__"
  val DEFAULT_TOP_N = 1000

  def emptyArrayIfMissing(query: String): String = {
    if (query.trim == "") {
      "array()"
    } else {
      query
    }
  }

  def generateWarnCases(dataQualityCheckMapping: Seq[DataQualityConfig]): String = {
    emptyArrayIfMissing(dataQualityCheckMapping
      .filter(_.errorType == ErrorType.warn)
      .map(it => s"""CASE WHEN ${it.rule} THEN array("${it.dataCheckType}${DELIMITER}${it.column}") ELSE array() END""")
      .mkString(",\n\t\t\t\t")
    )
  }

  def generateErrorCases(dataQualityCheckMapping: Seq[DataQualityConfig]): String = {
    emptyArrayIfMissing(dataQualityCheckMapping
      .filter(_.errorType == ErrorType.error)
      .map(it => s"""CASE WHEN ${it.rule} THEN array("${it.dataCheckType}${DELIMITER}${it.column}") ELSE array() END""")
      .mkString(",\n\t\t\t\t")
    )
  }

  def joinIdColumns(idColumn: String, prefix: String = ""): String = {
    val realPrefix = if (isNullOrEmpty(prefix)) "" else s"`$prefix`."
    if (idColumn.contains(",")) {
      idColumn.split(",").map(it => s"ifnull($realPrefix`${it.trim}`, 'NULL')").mkString("CONCAT(", s", '$DELIMITER', ", ")")
    } else {
      s"$realPrefix`$idColumn`"
    }
  }

  def joinOnConditions(resultView: String, tempViewName: String, idColumn: String): String = {
    idColumn.split(",").map(_.trim).zipWithIndex.map { case (column: String, idx: Int) =>
      s"""`$tempViewName`.`$column` = split(`$resultView`.id, '$DELIMITER')[$idx]"""
    }.mkString(" AND \n\t")
  }

  def generateAntiJoinSql(sql: String, udrSql: String, tempViewName: String): String = {
    if (isNullOrEmpty(sql) && isNullOrEmpty(udrSql)) {
      s"""SELECT * FROM `$tempViewName`"""
    } else {
      s"""|SELECT `$tempViewName`.* FROM `$tempViewName`
          |$sql
          |$udrSql
       """.stripMargin
    }
  }
}
