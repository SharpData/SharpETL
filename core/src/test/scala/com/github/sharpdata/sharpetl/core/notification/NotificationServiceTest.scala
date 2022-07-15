package com.github.sharpdata.sharpetl.core.notification

import com.github.sharpdata.sharpetl.core.repository.JobLogAccessor
import com.github.sharpdata.sharpetl.core.repository.model.{JobLog, JobStatus, StepLog}
import com.github.sharpdata.sharpetl.core.util.ETLConfig
import com.github.sharpdata.sharpetl.core.notification.sender.NotificationFactory
import com.github.sharpdata.sharpetl.core.repository.JobLogAccessor
import com.github.sharpdata.sharpetl.core.repository.model.{JobLog, JobStatus, StepLog}
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, Failure, Success}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, times, verify, when}
import org.mockito.MockitoSugar.withObjectMocked
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.time.LocalDateTime

class NotificationServiceTest extends AnyFlatSpec with should.Matchers {

  it should "send notification correctly when config notification setting" in {

    withObjectMocked[NotificationFactory.type] {
      val path = getClass.getResource("/application.properties").toString
      ETLConfig.setPropertyPath(path)
      val jobLogAccessor = mock(classOf[JobLogAccessor])
      val service = new NotificationService(jobLogAccessor)

      val job1 = mockJobLog("job1", 1, JobStatus.FAILURE)
      job1.setStepLogs(Array(mockStepLog(1, "1", JobStatus.FAILURE)))

      val job2 = mockJobLog("job2", 2, JobStatus.FAILURE)
      job2.setStepLogs(Array(mockStepLog(2, "1", JobStatus.SUCCESS), mockStepLog(2, "2", JobStatus.FAILURE)))

      service.sendNotification(Seq(Seq(Failure(job1, new RuntimeException("???"))), Seq(Failure(job2, new RuntimeException("???")))))
      verify(NotificationFactory, times(2)).sendNotification(any())
    }

  }

  it should "send notification correctly when no previous executed jobLog" in {

    withObjectMocked[NotificationFactory.type] {
      val path = getClass.getResource("/application.properties").toString
      ETLConfig.setPropertyPath(path)
      val jobLogAccessor = mock(classOf[JobLogAccessor])
      val service = new NotificationService(jobLogAccessor)

      val jobLog = mockJobLog("job2", 2, JobStatus.FAILURE)
      jobLog.setStepLogs(Array(mockStepLog(2, "1", JobStatus.FAILURE)))

      service.sendNotification(Seq(Seq(Success(jobLog))))
      verify(NotificationFactory, times(1)).sendNotification(any())
    }

  }

  it should "send notification correctly when trigger condition is failure and last executed success" in {

    withObjectMocked[NotificationFactory.type] {
      val path = getClass.getResource("/application.properties").toString
      ETLConfig.setPropertyPath(path)
      val jobLogAccessor = mock(classOf[JobLogAccessor])
      val service = new NotificationService(jobLogAccessor)

      val jobLog = mockJobLog("job2", 2, JobStatus.FAILURE)
      jobLog.setStepLogs(Array(mockStepLog(2, "1", JobStatus.FAILURE)))
      val previousJobLog = mockJobLog("job2", 1, JobStatus.SUCCESS)

      when(jobLogAccessor.getPreviousJobLog(jobLog))
        .thenReturn(previousJobLog)

      service.sendNotification(Seq(Seq(Success(jobLog))))
      verify(NotificationFactory, times(1)).sendNotification(any())
    }

  }

  it should "send notification correctly when trigger condition is failure and last executed failed " in {

    withObjectMocked[NotificationFactory.type] {
      val path = getClass.getResource("/application.properties").toString
      ETLConfig.setPropertyPath(path)
      val jobLogAccessor = mock(classOf[JobLogAccessor])
      val service = new NotificationService(jobLogAccessor)

      val jobLog = mockJobLog("job2", 2, JobStatus.FAILURE)
      jobLog.setStepLogs(Array(mockStepLog(2, "1", JobStatus.FAILURE)))
      val previousJobLog = mockJobLog("job2", 1, JobStatus.FAILURE)

      when(jobLogAccessor.getPreviousJobLog(jobLog))
        .thenReturn(previousJobLog)

      service.sendNotification(Seq(Seq(Success(jobLog))))
      verify(NotificationFactory, times(0)).sendNotification(any())
    }

  }

  private def mockJobLog(jobName: String, jobId: Long, status: String): JobLog = {
    new JobLog(
      jobId = jobId,
      jobName = jobName,
      jobPeriod = 1440,
      jobScheduleId = "20221111",
      dataRangeEnd = "20211212000000",
      dataRangeStart = "20211211000000",
      jobStartTime = LocalDateTime.now(),
      jobEndTime = LocalDateTime.now(),
      status = status,
      createTime = LocalDateTime.now(),
      lastUpdateTime = LocalDateTime.now(),
      incrementalType = "",
      currentFile = "",
      applicationId = "fake-app-001",
      projectName = ""
    )
  }

  private def mockStepLog(jobId: Long, stepId: String, status: String): StepLog = {
    new StepLog(
      jobId = jobId,
      stepId = stepId,
      status = status,
      startTime = LocalDateTime.now(),
      endTime = LocalDateTime.now(),
      duration = 10,
      output = "",
      error = "",
      successCount = 10,
      sourceCount = 10,
      targetCount = 10,
      failureCount = 10,
      sourceType = "",
      targetType = ""
    )
  }
}