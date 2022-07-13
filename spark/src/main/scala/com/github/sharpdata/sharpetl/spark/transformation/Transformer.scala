package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.core.annotation.Annotations.Stable
import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import org.apache.spark.sql.DataFrame


@Stable(since = "1.0.0")
trait Transformer {

  /**
   * read
   */
  def transform(args: Map[String, String]): DataFrame = ???

  /**
   * write
   */
  def transform(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = ???
}
