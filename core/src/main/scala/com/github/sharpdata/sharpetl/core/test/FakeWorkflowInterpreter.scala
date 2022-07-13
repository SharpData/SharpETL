package com.github.sharpdata.sharpetl.core.test

import com.github.sharpdata.sharpetl.core.api.{Variables, WorkflowInterpreter}
import com.github.sharpdata.sharpetl.core.quality.{DataQualityCheckResult, QualityCheckRule}
import com.github.sharpdata.sharpetl.core.repository.QualityCheckAccessor
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep


// $COVERAGE-OFF$
class FakeWorkflowInterpreter extends WorkflowInterpreter[Seq[_]] {
  override def listFiles(steps: List[WorkflowStep], step: WorkflowStep): List[String] = List()

  override def deleteSource(step: WorkflowStep): Unit = ()

  override def readFile(step: WorkflowStep, jobLog: JobLog,
                        variables: Variables,
                        files: List[String]): Seq[_] = List()

  override def executeWrite(jobLog: JobLog, df: Seq[_], step: WorkflowStep, variables: Variables): Unit = ()

  override def executeRead(step: WorkflowStep, jobLog: JobLog, variables: Variables): Seq[_] = Seq()

  override val qualityCheckAccessor: QualityCheckAccessor = new com.github.sharpdata.sharpetl.core.repository.mysql.QualityCheckAccessor()

  override val dataQualityCheckRules: Map[String, QualityCheckRule] = Map()

  override def createView(df: Seq[_], tempViewName: String): Unit = ???

  override def dropView(tempViewName: String): Unit = ???

  override def execute(sql: String): Seq[_] = ???

  override def queryCheckResult(sql: String): Seq[DataQualityCheckResult] = ???

  override def applicationId(): String = "fake-app-001"

  override def transformListFiles(filePaths: List[String]): Seq[_] = ???

  override def dropUnusedCols(df: Seq[_], cols: String): Seq[_] = ???
}
// $COVERAGE-ON$
