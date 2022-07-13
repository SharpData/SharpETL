package com.github.sharpdata.sharpetl.core.datasource

import com.github.sharpdata.sharpetl.core.annotation.Annotations.Stable
import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep

// scalastyle:off
@Stable(since = "1.0.0")
trait Source[DataFrame, Context] extends Serializable {
  def read(step: WorkflowStep, jobLog: JobLog, executionContext: Context, variables: Variables): DataFrame
}
// scalastyle:on
