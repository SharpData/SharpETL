package com.github.sharpdata.sharpetl.core.exception

import java.io.{PrintWriter, StringWriter}

object Exception {

  final case class EmptyDataException(message: String, step: String) extends Exception(message)

  final case class IncrementalDiffModeTooMuchDataException(message: String) extends RuntimeException(message)

  final case class StepFailedException(throwable: Throwable, step: String) extends Exception(throwable)

  final case class AnotherJobIsRunningException(message: String) extends RuntimeException(message)

  final case class JobDependenciesError(message: String) extends RuntimeException(message)

  final case class IncompleteDataSourceException(message: String) extends Exception(message)

  final case class DataQualityCheckRuleMissingException(message: String) extends RuntimeException(message)

  final case class BadDataQualityCheckRuleException(message: String) extends RuntimeException(message)

  final case class MissingConfigurationException(name: String) extends RuntimeException(s"configuration $name is missing")

  final case class UnsupportedStreamingDataSourceException(name: String)
    extends RuntimeException(s"datasource $name is not supported with streaming job")

  final case class FileDataSourceConfigErrorException(msg: String) extends RuntimeException(msg)

  final case class CanNotLoadPropertyFileException(message: String, e: Throwable) extends RuntimeException(message, e)

  final case class FailedToParseExtraParamsException(message: String) extends RuntimeException(message)

  final case class SheetNotFoundException(message: String) extends RuntimeException(message)

  final case class CellNotFoundException(headerName: String)
    extends RuntimeException(s"header `$headerName` does not exist in current excel sheet")

  final case class NoFileFoundException(step: String) extends RuntimeException("No file need to executed!")

  final case class NoFileToContinueException(step: String) extends RuntimeException("No file found to execute follow step!")

  final case class NoFileSkipException(step: String) extends RuntimeException("No file found, then skip follow step!")

  final case class InvalidSqlException(msg: String) extends RuntimeException(msg)

  final case class DuplicatedSqlScriptException(msg: String) extends RuntimeException(msg)

  final case class BadYamlFileException(msg: String) extends RuntimeException(msg)

  final case class WorkFlowSyntaxException(msg: String) extends RuntimeException(msg)

  final class CheckFailedException(msg: String) extends RuntimeException(msg)

  def throwableAsString(t: Throwable): String = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }
  final class PartitionNotFoundException(msg: String) extends RuntimeException(msg)
}
