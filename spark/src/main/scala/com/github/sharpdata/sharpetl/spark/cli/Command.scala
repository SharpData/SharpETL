package com.github.sharpdata.sharpetl.spark.cli

import com.github.sharpdata.sharpetl.modeling.cli.{GenerateDwdStepCommand, GenerateSqlFiles}
import com.github.sharpdata.sharpetl.spark.utils.JavaVersionChecker
import com.github.sharpdata.sharpetl.core.api
import com.github.sharpdata.sharpetl.core.api.WFInterpretingResult.checkSuccessOrThrow
import com.github.sharpdata.sharpetl.core.api.{LogDrivenInterpreter, WFInterpretingResult}
import com.github.sharpdata.sharpetl.core.cli.{BatchJobCommand, EncryptionCommand, SingleJobCommand}
import com.github.sharpdata.sharpetl.core.notification.NotificationUtil
import com.github.sharpdata.sharpetl.core.quality.QualityCheckRuleConfig.readQualityCheckRules
import com.github.sharpdata.sharpetl.core.repository.JobLogAccessor.jobLogAccessor
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.util.FlywayUtil.migrate
import com.github.sharpdata.sharpetl.core.util._
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.getSparkInterpreter
import picocli.CommandLine


@CommandLine.Command(name = "single-job")
class SingleSparkJobCommand extends SingleJobCommand {
  override def run(): Unit = {
    loggingJobParameters()
    ETLConfig.extraParam = extraParams
    ETLConfig.setPropertyPath(propertyPath, env)
    val etlDatabaseType = JDBCUtil.dbType
    migrate()
    val interpreter = getSparkInterpreter(local, jobName, releaseResource, etlDatabaseType, readQualityCheckRules())
    JavaVersionChecker.checkJavaVersion()
    try {
      val wfInterpretingResult: WFInterpretingResult = api.LogDrivenInterpreter(
        jobName,
        WorkflowReader.readWorkflow(jobName),
        interpreter,
        additionalProperties = Map(("defaultStart", defaultStart)),
        jobLogAccessor = jobLogAccessor,
        command = this
      ).interpreting()
      new NotificationUtil(jobLogAccessor).notify(Seq(wfInterpretingResult))
      checkSuccessOrThrow(Seq(wfInterpretingResult))
    } finally {
      interpreter.close()
    }
  }
}

@CommandLine.Command(name = "batch-job")
class BatchSparkJobCommand extends BatchJobCommand {
  override def run(): Unit = {
    loggingJobParameters()
    ETLConfig.extraParam = extraParams
    ETLConfig.setPropertyPath(propertyPath, env)
    JavaVersionChecker.checkJavaVersion()
    migrate()
    val etlDatabaseType = JDBCUtil.dbType
    // val logDrivenInterpreters = if (excelOptions != null) getJobsFromExcel(etlDatabaseType) else getInterpretersFromSqlFile(etlDatabaseType)
    val logDrivenInterpreters = getInterpretersFromSqlFile(etlDatabaseType)
    val batchJobResult: Seq[WFInterpretingResult] =
      try {
        logDrivenInterpreters.map(_.interpreting())
      } finally {
        logDrivenInterpreters.headOption.foreach(_.workflowInterpreter.close())
      }
    val failedCount = batchJobResult.map(_.jobLogs.count { it => it.isFailure() }).sum
    val skippedCount = batchJobResult.map(_.jobLogs.count { it => it.isSkipped() }).sum
    val successCount = batchJobResult.map(_.jobLogs.count { it => it.isSuccess() }).sum

    ETLLogger.info(
      s"""
         |Total jobs: ${logDrivenInterpreters.size}, success: $successCount, failed: $failedCount, skipped: $skippedCount
         |Details:
         |${batchJobResult.map(_.toString).mkString("\n\n")}
         |""".stripMargin)
    new NotificationUtil(jobLogAccessor).notify(batchJobResult)
    if (failedCount > 0) {
      checkSuccessOrThrow(batchJobResult)
    }
  }

  def getInterpretersFromSqlFile(etlDatabaseType: String): Seq[LogDrivenInterpreter] = {
    sqlFileOptions.jobNames
      .map(jobName => {
        val interpreter = getSparkInterpreter(local, jobName, releaseResource, etlDatabaseType, readQualityCheckRules())
        JavaVersionChecker.checkJavaVersion()
        LogDrivenInterpreter(
          jobName,
          WorkflowReader.readWorkflow(jobName),
          interpreter,
          additionalProperties = Map(("defaultStart", defaultStart)),
          jobLogAccessor = jobLogAccessor,
          command = this
        )
      })
  }
}

@CommandLine.Command(
  subcommands = Array(
    classOf[SingleSparkJobCommand],
    classOf[BatchSparkJobCommand],
    classOf[GenerateSqlFiles],
    classOf[EncryptionCommand],
    classOf[GenerateDwdStepCommand]
  )
)
class Command extends Runnable {

  override def run(): Unit = ()

}
