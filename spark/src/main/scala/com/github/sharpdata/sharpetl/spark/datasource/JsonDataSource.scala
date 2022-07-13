package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.Source
import com.github.sharpdata.sharpetl.core.datasource.config.JsonDataSourceConfig
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.StringUtil
import com.github.sharpdata.sharpetl.core.annotation._
import org.apache.spark.sql.{DataFrame, SparkSession}

@source(types = Array("json"))
class JsonDataSource extends Source[DataFrame, SparkSession] {

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    val sourceConfig = step.getSourceConfig[JsonDataSourceConfig]
    executionContext
      .read
      .option("multiline", sourceConfig.getMultiline.toBoolean)
      .option("mode", sourceConfig.getMode)
      .options(sourceConfig.getOptions())
      .json(sourceConfig.getFilePath)
      .selectExpr(
        s"'${StringUtil.getFileNameFromPath(sourceConfig.getFilePath)}' as file_name",
        "*"
      )
  }
}
