package com.github.sharpdata.sharpetl.datasource.kudu

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.annotation._
import org.apache.spark.sql.{DataFrame, SparkSession}

@source(types = Array("kudu", "impala_kudu"))
@sink(types = Array("kudu", "impala_kudu"))
class KuduDataSource extends Source[DataFrame, SparkSession] with Sink[DataFrame] {
  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = ???

  override def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = ???
}
