package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.spark.end2end.ETLSuit
import com.github.sharpdata.sharpetl.core.notification.sender.NotificationFactory
import com.github.sharpdata.sharpetl.core.notification.sender.email.Email
import com.github.sharpdata.sharpetl.core.repository.JobLogAccessor.jobLogAccessor
import com.github.sharpdata.sharpetl.core.repository.StepLogAccessor.stepLogAccessor
import com.github.sharpdata.sharpetl.core.repository.model.{JobLog, JobStatus, StepLog}
import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType
import com.github.sharpdata.sharpetl.core.util.DateUtil.L_YYYY_MM_DD_HH_MM_SS
import com.github.sharpdata.sharpetl.core.util.FlywayUtil
import ETLSuit.runJob
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{times, verify}
import org.mockito.MockitoSugar.withObjectMocked
import org.scalatest.DoNotDiscover

import java.time.LocalDateTime
import java.util.TimeZone

@DoNotDiscover
class DailyJobsSummaryReportTransformSpec extends ETLSuit {
  override val createTableSql: String = ""

  it("should send summary report correct") {
    val timeZone = System.getProperty("user.timezone")
    TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"))
    val startTime = LocalDateTime.parse("2022-01-01 10:00:00", L_YYYY_MM_DD_HH_MM_SS)
    val startTimeText = startTime.format(L_YYYY_MM_DD_HH_MM_SS)

    FlywayUtil.migrate()
    prepareData("job1", startTime.plusHours(10))
    prepareData("job2", startTime.plusHours(10))

    withObjectMocked[NotificationFactory.type] {

      val jobParameters: Array[String] = Array("single-job",
        "--name=daily_jobs_summary_report_test", "--period=1440",
        "--local", s"--default-start-time=$startTimeText", "--env=test", "--once")

      runJob(jobParameters)
      val argument = ArgumentCaptor.forClass(classOf[Email])
      verify(NotificationFactory, times(1)).sendNotification(argument.capture())
      val email = argument.getValue.asInstanceOf[Email]
      val emptyStr = ""

      assert(email.attachment.get.content ==
        s"""projectName,workflowName,jobId,dataRangeStart,dataRangeEnd,jobStartTime,jobStatus,duration(seconds),dataFlow,to-hive,to-postgres,failStep,errorMessage
          |projectName,job1,1,2022-02-09 00:00:00,2022-02-10 00:00:00,2022-01-01 20:00:00,SUCCESS,20,hive(10) -> postgres(10),10,10,,""
          |projectName,job2,2,2022-02-09 00:00:00,2022-02-10 00:00:00,2022-01-01 20:00:00,SUCCESS,20,hive(10) -> postgres(10),10,10,,""$emptyStr""".stripMargin
      )
    }
    TimeZone.setDefault(TimeZone.getTimeZone(timeZone))
  }

  private def prepareData(jobName: String, jobStartTime: LocalDateTime): Unit = {
    val jobLog = mockJobLog(jobName, jobStartTime)
    jobLogAccessor.create(jobLog) // create time will converted to now, so let's do update
    jobLog.jobStartTime = jobStartTime
    jobLogAccessor.update(jobLog)

    stepLogAccessor.create(
      mockStepLog(jobLog.jobId, "1", DataSourceType.HIVE, 10, jobStartTime)
    )
    stepLogAccessor.create(
      mockStepLog(jobLog.jobId, "2", DataSourceType.POSTGRES, 5, jobStartTime)
    )
  }

  private def mockJobLog(wfName: String, jobStartTime: LocalDateTime): JobLog = {
    new JobLog(
      jobId = "1",
      workflowName = wfName,
      period = 1440,
      jobName = "111",
      dataRangeStart = "20220209000000",
      dataRangeEnd = "20220210000000",
      jobStartTime = jobStartTime,
      jobEndTime = jobStartTime,
      status = JobStatus.SUCCESS,
      createTime = LocalDateTime.now(),
      lastUpdateTime = LocalDateTime.now(),
      logDrivenType = "",
      file = "",
      projectName = "projectName",
      applicationId = "applicationId",
      loadType = "",
      runtimeArgs = ""
    )
  }

  private def mockStepLog(jobId: String, stepId: String, targetType: String, targetCount: Int, startTime: LocalDateTime): StepLog = {
    new StepLog(
      jobId = jobId,
      stepId = stepId,
      status = JobStatus.SUCCESS,
      startTime = startTime,
      endTime = startTime,
      duration = 10,
      output = "",
      error = "",
      sourceCount = 10,
      targetCount = targetCount,
      successCount = 10,
      failureCount = 0,
      sourceType = "HIVE",
      targetType = targetType)
  }
}
