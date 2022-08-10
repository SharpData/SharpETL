package com.github.sharpdata.sharpetl.spark.datasource

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.Sink
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.spark.utils.VariablesUtil
import org.apache.spark.sql.DataFrame

@source(types = Array("variables"))
@sink(types = Array("variables"))
class VariablesDataSource extends Sink[DataFrame] {
  override def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    VariablesUtil.setVariables(df, variables)
  }
}
