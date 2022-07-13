package com.github.sharpdata.sharpetl.spark.cli

import com.github.sharpdata.sharpetl.modeling.cli.{GenerateDwdStepCommand, GenerateSqlFiles}
import com.github.sharpdata.sharpetl.spark.utils.JavaVersionChecker
import com.github.sharpdata.sharpetl.core.api
import com.github.sharpdata.sharpetl.core.api.LogDrivenJob
import com.github.sharpdata.sharpetl.core.cli.{BatchJobCommand, EncryptionCommand, SingleJobCommand}
import com.github.sharpdata.sharpetl.core.notification.NotificationService
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
      val result: Seq[Try[JobLog]] = api.LogDrivenJob(
        jobName,
        WorkflowReader.readWorkflow(jobName),
        interpreter,
        additionalProperties = Map(("defaultStart", defaultStart)),
        jobLogAccessor = jobLogAccessor,
        command = this
      ).exec()
      new NotificationService(jobLogAccessor)
        .sendNotification(Seq(result))
      result.find(it => it.isFailure()).foreach {
        case Failure(_, throwable) => throw throwable
        case _ => () //not possible here
      }
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
    // val jobs = if (excelOptions != null) getJobsFromExcel(etlDatabaseType) else getJobsFromSqlFile(etlDatabaseType)
    val jobs = getJobsFromSqlFile(etlDatabaseType)
    val batchJobResult: Seq[Seq[Try[JobLog]]] =
      try {
        jobs.map(_.exec())
      } finally {
        jobs.headOption.foreach(_.jobInterpreter.close())
      }
    val failedCount = batchJobResult.map(_.count { it => it.isFailure() }).sum
    val skippedCount = batchJobResult.map(_.count { it => it.isSkipped() }).sum
    val successCount = batchJobResult.map(_.count { it => it.isSuccess() }).sum

    ETLLogger.info(
      s"""
         |Total jobs: ${jobs.size}, success: $successCount, failed: $failedCount, skipped: $skippedCount
         |Details:
         |${batchJobResult.flatMap { s => formatString(s) }.mkString("\n")}
         |""".stripMargin)
    new NotificationService(jobLogAccessor)
      .sendNotification(batchJobResult)
    if (failedCount > 0) {
      batchJobResult.flatten.find(it => it.isFailure()).foreach {
        case Failure(_, throwable) => throw throwable
        case _ => () //not possible here
      }
    }
  }

  def getJobsFromSqlFile(etlDatabaseType: String): Seq[LogDrivenJob] = {
    sqlFileOptions.jobNames
      .map(jobName => {
        val interpreter = getSparkInterpreter(local, jobName, releaseResource, etlDatabaseType, readQualityCheckRules())
        JavaVersionChecker.checkJavaVersion()
        api.LogDrivenJob(
          jobName,
          WorkflowReader.readWorkflow(jobName),
          interpreter,
          additionalProperties = Map(("defaultStart", defaultStart)),
          jobLogAccessor = jobLogAccessor,
          command = this
        )
      })
  }

  def formatString(jobs: Seq[Try[JobLog]]): Seq[String] = {
    if (jobs.nonEmpty) {
      val jobName = jobs.head.get.jobName
      jobs.groupBy(_.getClass.getSimpleName)
        .map { case (status, seq) =>
          status match {
            case "Success" => s"""job name: $jobName SUCCESS x ${seq.size}"""
            case "Failure" =>
              s"""job name: $jobName FAILURE x ${seq.size}, job id: ${seq.head.asInstanceOf[Failure[JobLog]].result.jobId}
                 | error: ${seq.head.asInstanceOf[Failure[JobLog]].e.getMessage}""".stripMargin
            case "Skipped" =>
              s"""job name: $jobName SKIPPED x ${seq.size}
                 |from data range start ${seq.head.asInstanceOf[Skipped[JobLog]].result.dataRangeStart}""".stripMargin
          }
        }.toSeq
    } else {
      Seq()
    }
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
