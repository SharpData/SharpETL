package com.github.sharpdata.sharpetl.spark.quality

import com.github.sharpdata.sharpetl.core.annotation.Annotations.Stable
import com.github.sharpdata.sharpetl.core.quality.QualityCheck._
import com.github.sharpdata.sharpetl.core.quality.{DataQualityCheckResult, QualityCheck, QualityCheckRule}
import com.github.sharpdata.sharpetl.core.repository.QualityCheckAccessor
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.spark.utils.Encoder.dqEncoder
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.jdk.CollectionConverters._

@Stable(since = "1.0.0")
class SparkQualityCheck(val spark: SparkSession,
                        override val dataQualityCheckRules: Map[String, QualityCheckRule],
                        override val qualityCheckAccessor: QualityCheckAccessor)
  extends QualityCheck[DataFrame] {

  override def queryCheckResult(sql: String): Seq[DataQualityCheckResult] = {
    if (sql.trim == "") {
      Seq()
    } else {
      ETLLogger.info(s"execution sql:\n $sql")
      spark.sql(sql).as[DataQualityCheckResult](dqEncoder).collectAsList().asScala
        .map(it => DataQualityCheckResult(it.column, it.dataCheckType, it.ids, it.errorType.split(DELIMITER).head, it.warnCount, it.errorCount))
        .filterNot(it => it.warnCount < 1 && it.errorCount < 1)
        .toSeq
    }
  }

  override def execute(sql: String): DataFrame = {
    ETLLogger.info(s"Execution sql: \n $sql")
    spark.sql(sql)
  }

  override def createView(df: DataFrame, tempViewName: String): Unit = {
    ETLLogger.info(s"Creating temp view `$tempViewName`")
    df.createOrReplaceTempView(s"`$tempViewName`")
  }

  override def dropView(tempViewName: String): Unit = {
    ETLLogger.info(s"Dropping temp view `$tempViewName`")
    spark.catalog.dropTempView(s"`$tempViewName`")
  }

  override def dropUnusedCols(df: DataFrame, cols: String): DataFrame = {
    df.drop(cols.split(",").map(_.trim): _*)
  }
}
