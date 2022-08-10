package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.Sink
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.annotation._
import org.apache.spark.sql.DataFrame

@sink(types = Array("console"))
class ConsoleDataSource extends Sink[DataFrame] {
  override def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    df.printSchema()
    df.show(numRows = 100 * 100, truncate = false)
  }
}
