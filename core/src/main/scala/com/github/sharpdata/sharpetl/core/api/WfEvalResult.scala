package com.github.sharpdata.sharpetl.core.api

import com.github.sharpdata.sharpetl.core.annotation.Annotations.Evolving
import com.github.sharpdata.sharpetl.core.repository.model.JobLog
import com.github.sharpdata.sharpetl.core.syntax.{Formatable, Workflow}
import com.github.sharpdata.sharpetl.core.util.{Failure, Skipped, Try}

@Evolving("1.0.0")
final case class WfEvalResult(workflow: Workflow, jobLogs: Seq[Try[JobLog]]) extends Formatable {

  override def toString: String = formatString.mkString("\n")

  def formatString: Seq[String] = {
    if (jobLogs.nonEmpty) {
      val workflowName = jobLogs.head.get.workflowName
      jobLogs.groupBy(_.getClass.getSimpleName)
        .map { case (status, seq) =>
          status match {
            case "Success" => s"""workflow name: $workflowName SUCCESS x ${seq.size}"""
            case "Failure" =>
              s"""workflow name: $workflowName FAILURE x ${seq.size}, job id: ${seq.head.asInstanceOf[Failure[JobLog]].result.jobId}
                 | error: ${seq.head.asInstanceOf[Failure[JobLog]].e.getMessage}""".stripMargin
            case "Skipped" =>
              s"""workflow name: $workflowName SKIPPED x ${seq.size}
                 |from data range start ${seq.head.asInstanceOf[Skipped[JobLog]].result.dataRangeStart}""".stripMargin
          }
        }.toSeq
    } else {
      Seq()
    }
  }
}

object WfEvalResult {
  def throwFirstException(results: Seq[WfEvalResult]): Unit = {
    results
      .flatMap(_.jobLogs)
      .foreach {
        case Failure(_, throwable) => throw throwable
        case _ => ()
      }
  }
}
