package com.github.sharpdata.sharpetl.core.util

import com.github.sharpdata.sharpetl.core.util.Constants.PathPrefix
import com.github.sharpdata.sharpetl.core.exception.Exception.{DuplicatedSqlScriptException, WorkFlowSyntaxException}
import com.github.sharpdata.sharpetl.core.syntax._

import java.io.{File, FileNotFoundException}

object WorkflowReader {

  def readWorkflow(workflowName: String): Workflow = {
    val lines = readLines(workflowName)
    WorkflowParser.parseWorkflow(lines.mkString("\n")) match {
      case success: WFParseSuccess => success.wf
      case fail: WFParseFail =>
        throw WorkFlowSyntaxException(fail.toString)
    }
  }

  def readLines(workflowName: String): List[String] = {
    val configRootDir = ETLConfig.getProperty("etl.workflow.path")
    val pathPrefix = getPathPrefix(configRootDir)
    val (taskPathMapping, duplicatedFileNames) = readTaskPathMapping(pathPrefix, configRootDir)
    if (duplicatedFileNames.nonEmpty && duplicatedFileNames.keySet.contains(workflowName)) {
      throw DuplicatedSqlScriptException(
        s"""There are multiple files have the same filename: $workflowName, paths ${duplicatedFileNames(workflowName).mkString(",\n")}
           |Please check your workflow folder and delete the duplicated file.""".stripMargin
      )
    }
    if (taskPathMapping.isDefinedAt(workflowName)) {
      val taskPath = taskPathMapping(workflowName)
      val lines = pathPrefix match {
        case PathPrefix.FILE =>
          IOUtil.readLinesFromText(taskPath)
        case PathPrefix.HDFS | PathPrefix.DBFS =>
          HDFSUtil.readLines(taskPath)
      }
      lines
    } else {
      throw new FileNotFoundException(s"Workflow or transformer '$workflowName.sql/.scala' not found.")
    }
  }

  def getPathPrefix(configRootDir: String): String = {
    Option(configRootDir)
      .filter(_.indexOf(":") >= 0)
      .map(_.substring(0, configRootDir.indexOf(":")))
      .getOrElse("")
  }

  type MappingWithDuplicatedList = (Map[String, String], Map[String, Seq[String]])

  val readTaskPathMapping: Memo2[String, String, MappingWithDuplicatedList] =
    Memo2 { (pathPrefix: String, configRootDir: String) => doReadTaskPathMapping(pathPrefix, configRootDir) }

  private def doReadTaskPathMapping(pathPrefix: String, configRootDir: String): MappingWithDuplicatedList = {
    val fileNameToPath: Seq[(String, String)] = Seq(pathPrefix)
      .flatMap {
        case PathPrefix.FILE =>
          IOUtil.recursiveListFilesFromResource(configRootDir)
        case PathPrefix.HDFS | PathPrefix.DBFS =>
          HDFSUtil.recursiveListFiles(configRootDir)
      }
      .map(path => path.substring(path.lastIndexOf(File.separator) + 1, path.lastIndexOf(".")) -> path)
    (fileNameToPath.toMap, duplicatedFileNames(fileNameToPath))
  }

  private def duplicatedFileNames(fileNameToPath: Seq[(String, String)]) = {
    fileNameToPath.groupBy(_._1).filter(_._2.size > 1).map {
      case (fileName, list) => (fileName, list.map(_._2))
    }
  }
}
