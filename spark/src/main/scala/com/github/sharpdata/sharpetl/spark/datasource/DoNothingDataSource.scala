package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.{Sink, Source}
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.annotation._
import org.apache.spark.sql.{DataFrame, SparkSession}

@source(types = Array("do_nothing"))
@sink(types = Array("do_nothing"))
class DoNothingDataSource extends Source[DataFrame, SparkSession] with Sink[DataFrame] {
  override def read(step: WorkflowStep, jobLog: JobLog, executionContext: SparkSession, variables: Variables): DataFrame = {
    import executionContext.implicits._
    executionContext.sparkContext.parallelize(Seq("")).toDF("line")
  }

  override def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = ()
}
