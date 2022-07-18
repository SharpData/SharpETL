package com.github.sharpdata.sharpetl.core.api

import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.{Formatable, Workflow}
import com.github.sharpdata.sharpetl.core.util.{Failure, Skipped, Try}

final case class WFInterpretingResult(workflow: Workflow, jobLogs: Seq[Try[JobLog]]) extends Formatable {

  override def toString: String = formatString.mkString("\n")

  def formatString: Seq[String] = {
    if (jobLogs.nonEmpty) {
      val jobName = jobLogs.head.get.jobName
      jobLogs.groupBy(_.getClass.getSimpleName)
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

object WFInterpretingResult {
  def checkSuccessOrThrow(results: Seq[WFInterpretingResult]): Unit = {
    results
      .flatMap(_.jobLogs)
      .find(it => it.isFailure()).foreach {
      case Failure(_, throwable) => throw throwable
      case _ => () //not possible here
    }
  }
}
