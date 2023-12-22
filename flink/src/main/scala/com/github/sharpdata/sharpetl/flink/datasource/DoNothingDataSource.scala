package com.github.sharpdata.sharpetl.flink.datasource

import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.flink.job.Types.DataFrame
import com.github.sharpdata.sharpetl.flink.util.ETLFlinkSession
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

@source(types = Array("do_nothing"))
@sink(types = Array("do_nothing"))
class DoNothingDataSource extends Sink[DataFrame] with Source[DataFrame, StreamTableEnvironment]{
  override def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = ()

  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: StreamTableEnvironment, variables: Variables): DataFrame = {
    executionContext.toDataStream(executionContext.sqlQuery("SELECT 'SUCCESS' AS `result`"))
  }
}
