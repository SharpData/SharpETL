package com.github.sharpdata.sharpetl.flink.datasource

import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.flink.job.Types.DataFrame
import com.github.sharpdata.sharpetl.flink.util.ETLFlinkSession
import org.apache.flink.table.api.TableEnvironment

@source(types = Array("built_in"))
@sink(types = Array("built_in"))
class BuiltInDataSource extends Sink[DataFrame] with Source[DataFrame, TableEnvironment] {

  override def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    val sql = s"INSERT INTO ${step.target.asInstanceOf[DBDataSourceConfig].getTableName} ${step.getSql}"
    println("executing sql:\n " + sql)
    ETLFlinkSession.sparkSession.executeSql(sql)
  }

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: TableEnvironment, variables: Variables): DataFrame = {
    println("executing sql:\n " + step.getSql)
    executionContext.sqlQuery(step.getSql)
  }
}
