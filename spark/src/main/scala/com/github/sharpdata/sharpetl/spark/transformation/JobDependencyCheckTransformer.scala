package com.github.sharpdata.sharpetl.spark.transformation

import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.exception.Exception.JobDependenciesError
import com.github.sharpdata.sharpetl.core.repository.JobLogAccessor.jobLogAccessor
import com.github.sharpdata.sharpetl.core.util.DateUtil.{L_YYYY_MM_DD_HH_MM_SS, LocalDateTimeToBigInt, YYYYMMDDHHMMSS}
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.core.util.StringUtil.BigIntConverter
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

import scala.math.BigInt.javaBigInteger2bigInt

object JobDependencyCheckTransformer extends Transformer {
  override def transform(args: Map[String, String]): DataFrame = {
    val nextDataRangeEnd =  LocalDateTime.parse(args("dataRangeEnd"), L_YYYY_MM_DD_HH_MM_SS).asBigInt()
    val dependencies: String = args("dependencies")
    if (!isNullOrEmpty(dependencies)) {
      val jobNames = dependencies.split(",").map(_.trim)

      val jobName = args("workflowName")
      val dependLogs = jobLogAccessor
        .getLatestSuccessJobLogByNames(jobNames)
        .filter(log => log.dataRangeEnd.asBigInt >= nextDataRangeEnd)
      if (dependLogs.length != jobNames.length) {
        val diff = jobNames.diff(dependLogs.map(log => log.getWorkflowName))
        val errorMessage =
          s"""
             |Dependencies of job $jobName is not completed! Current job will not run.
             |Not completed dependencies: (${diff.mkString(",")})
             |""".stripMargin
        ETLLogger.error(errorMessage)
        throw JobDependenciesError(errorMessage)
      }
    }
    sparkSession.emptyDataFrame
  }
}
