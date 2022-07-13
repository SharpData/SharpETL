package com.github.sharpdata.sharpetl.spark.api

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.Source
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.annotation.AnnotationScanner
import com.github.sharpdata.sharpetl.spark.job.SparkSessionTestWrapper
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class AnnotationSpec extends AnyFunSpec with BeforeAndAfterEach with SparkSessionTestWrapper {

  it("find object by annotations") {
    val value: Class[Source[_, _]] = AnnotationScanner.sourceRegister("do_nothing")
    assert(value != null)

    value.getMethod("read", classOf[WorkflowStep], classOf[JobLog], classOf[SparkSession], classOf[Variables])
      .invoke(value.newInstance(), null, null, spark, null)
  }
}
