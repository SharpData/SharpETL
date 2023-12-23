package com.github.sharpdata.sharpetl.flink.quality

import com.github.sharpdata.sharpetl.core.annotation.Annotations.Stable
import com.github.sharpdata.sharpetl.core.quality.QualityCheck._
import com.github.sharpdata.sharpetl.core.quality.{DataQualityCheckResult, QualityCheck, QualityCheckRule}
import com.github.sharpdata.sharpetl.core.repository.QualityCheckAccessor
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.flink.job.Types.DataFrame
import org.apache.flink.table.api.Expressions._
import org.apache.flink.table.api.TableEnvironment

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
    tEnv.sqlQuery(sql)
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
}