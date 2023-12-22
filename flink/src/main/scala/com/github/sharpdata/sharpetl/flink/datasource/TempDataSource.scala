package com.github.sharpdata.sharpetl.flink.datasource

import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.flink.job.Types.DataFrame
import com.github.sharpdata.sharpetl.flink.util.ETLFlinkSession
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

@source(types = Array("temp"))
@sink(types = Array("temp"))
class TempDataSource extends Sink[DataFrame] with Source[DataFrame, StreamTableEnvironment] {

  override def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    ETLFlinkSession.sparkSession.createTemporaryView(step.target.asInstanceOf[DBDataSourceConfig].getTableName, df)
  }

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: StreamTableEnvironment, variables: Variables): DataFrame = {
    println("executing sql:\n " + step.getSql)
    executionContext.toDataStream(executionContext.sqlQuery(step.getSql))
  }
}
