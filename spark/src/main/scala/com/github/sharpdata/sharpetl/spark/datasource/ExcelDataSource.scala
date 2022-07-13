package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.Source
import com.github.sharpdata.sharpetl.core.datasource.config.ExcelDataSourceConfig
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.StringUtil
import com.github.sharpdata.sharpetl.core.annotation._
import org.apache.spark.sql.{DataFrame, SparkSession}

@source(types = Array("excel"))
class ExcelDataSource extends Source[DataFrame, SparkSession] {

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    // scalastyle:off
    val sourceConfig = step.getSourceConfig[ExcelDataSourceConfig]
    import com.crealytics.spark.excel._
    executionContext
      .read
      .options(sourceConfig.getOptions())
      .excel(
        header = sourceConfig.getHeader.toBoolean,
        treatEmptyValuesAsNulls = sourceConfig.getTreatEmptyValuesAsNulls.toBoolean,
        inferSchema = sourceConfig.getInferSchema.toBoolean,
        addColorColumns = sourceConfig.getAddColorColumns.toBoolean,
        dataAddress = sourceConfig.getDataAddress,
        timestampFormat = sourceConfig.getTimestampFormat,
        maxRowsInMemory = sourceConfig.getMaxRowsInMemory match {
          case s: String => s.toInt
          case _ => null
        },
        excerptSize = sourceConfig.getExcerptSize,
        workbookPassword = sourceConfig.getWorkbookPassword
      )
      .load(sourceConfig.getFilePath)
      .selectExpr(
        s"'${StringUtil.getFileNameFromPath(sourceConfig.getFilePath)}' as file_name",
        "*"
      )
    // scalastyle:on
  }

}
