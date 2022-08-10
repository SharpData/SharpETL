package com.github.sharpdata.sharpetl.core.datasource

import com.github.sharpdata.sharpetl.core.annotation.Annotations.Stable
import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep

// scalastyle:off
@Stable(since = "1.0.0")
trait Sink[DataFrame] extends Serializable {
  def write(df: DataFrame, step: WorkflowStep, variables: Variables): Unit
}
// scalastyle:on
