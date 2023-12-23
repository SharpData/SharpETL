package com.github.sharpdata.sharpetl.flink.datasource

import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.Sink
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.flink.job.Types.DataFrame
import com.github.sharpdata.sharpetl.flink.util.ETLFlinkSession

@sink(types = Array("console"))
class ConsoleDataSource extends Sink[DataFrame] {
  override def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    println("console output schema:")
    df.printSchema()
    println("explain plan:")
    println(df.explain())
    println("console output:")
    println(df.toString)
  }
}
