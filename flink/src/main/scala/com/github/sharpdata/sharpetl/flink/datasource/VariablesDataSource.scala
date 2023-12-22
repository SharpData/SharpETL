package com.github.sharpdata.sharpetl.flink.datasource

import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.Sink
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.flink.job.Types.DataFrame
import com.github.sharpdata.sharpetl.flink.util.VariablesUtil

@source(types = Array("variables"))
@sink(types = Array("variables"))
class VariablesDataSource extends Sink[DataFrame] {
  override def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    VariablesUtil.setVariables(df, variables)
  }
}
