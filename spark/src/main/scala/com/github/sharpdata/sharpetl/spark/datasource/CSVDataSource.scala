package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}
import com.github.sharpdata.sharpetl.core.datasource.config.CSVDataSourceConfig
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.HDFSUtil.mv
import com.github.sharpdata.sharpetl.core.util.{HDFSUtil, StringUtil}
import com.github.sharpdata.sharpetl.core.annotation._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.matching.Regex

@source(types = Array("csv"))
@sink(types = Array("csv"))
class CSVDataSource extends Source[DataFrame, SparkSession] with Sink[DataFrame] {

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    loadFromHdfs(executionContext, step.getSourceConfig)
  }

  override def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    save(df, step.getTargetConfig)
  }

  def loadFromHdfs(spark: SparkSession,
                   sourceConfig: CSVDataSourceConfig): DataFrame = {
    val df = spark
      .read
      .option("inferSchema", sourceConfig.getInferSchema)
      .option("encoding", sourceConfig.getEncoding)
      .option("sep", sourceConfig.getSep)
      .option("header", sourceConfig.getHeader)
      .option("quote", sourceConfig.getQuote)
      .option("escape", sourceConfig.getEscape)
      .option("multiLine", sourceConfig.getMultiLine)
      .option("ignoreTrailingWhiteSpace", sourceConfig.getIgnoreTrailingWhiteSpace)
      .csv(sourceConfig.filePath)
      .selectExpr(sourceConfig.getSelectExpr.split(","): _*)
      .withColumn("file_name", lit(StringUtil.getFileNameFromPath(sourceConfig.filePath)))

    if (!StringUtil.isNullOrEmpty(sourceConfig.parseTimeFromFileNameRegex)) {
      val filename = StringUtil.getFileNameFromPath(sourceConfig.filePath)
      val parseDate = sourceConfig.parseTimeColumnName
      val maybeMatch = new Regex(sourceConfig.parseTimeFromFileNameRegex, parseDate).findFirstMatchIn(filename)
      val parseDateStr = maybeMatch.get.group(parseDate)

      val dateFormat = DateTimeFormatter.ofPattern(sourceConfig.parseTimeFormatPattern)
      df.withColumn(parseDate, lit(Timestamp.valueOf(LocalDate.parse(parseDateStr, dateFormat).atStartOfDay())))
    } else {
      df
    }

  }

  def save(df: DataFrame,
           targetConfig: CSVDataSourceConfig): Unit = {
    val tempTargetPath = StringUtil.uuid
    val targetPath = targetConfig.getFilePath
    df
      .repartition(1)
      .write
      .option("encoding", targetConfig.getEncoding)
      .option("sep", targetConfig.getSep)
      .option("header", targetConfig.getHeader)
      .option("mapreduce.fileoutputcommitter.algorithm.version", "1")
      .mode(SaveMode.Overwrite)
      .csv(tempTargetPath)
    mv(
      HDFSUtil.listFileUrl(tempTargetPath, "part-00000.*").head,
      targetPath,
      overWrite = true
    )
  }
}
