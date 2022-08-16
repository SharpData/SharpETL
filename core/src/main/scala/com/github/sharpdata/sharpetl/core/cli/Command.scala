package com.github.sharpdata.sharpetl.core.cli

import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.exception.Exception.FailedToParseExtraParamsException
import com.github.sharpdata.sharpetl.core.util.Constants.Environment
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import org.apache.commons.io.FileUtils.byteCountToDisplaySize
import picocli.CommandLine
import picocli.CommandLine.ArgGroup

import scala.collection.mutable

abstract class CommonCommand extends Runnable {
  @CommandLine.Option(
    names = Array("--local"),
    description = Array("running in standalone mode"),
    required = false
  )
  var local: Boolean = false

  @CommandLine.Option(
    names = Array("--release-resource"),
    description = Array("Automatically release resource after job completion"),
    required = false
  )
  var releaseResource: Boolean = true

  @CommandLine.Option(
    names = Array("--skip-running"),
    description = Array("Handle the running job when scheduling flashed crash"),
    required = false
  )
  var skipRunning: Boolean = true

  @CommandLine.Option(
    names = Array("--default-start", "--default-start-time"),
    description = Array("Default start time(eg, 20210101000000)/incremental id of this job"),
    required = false
  )
  var defaultStart: String = _

  @CommandLine.Option(
    names = Array("--log-driven-type"),
    description = Array("log driven type"),
    required = false
  )
  var logDrivenType: String = _

  @CommandLine.Option(
    names = Array("--period"),
    description = Array("execute period of the job"),
    required = false
  )
  var period: Int = _

  @CommandLine.Option(
    names = Array("--once"),
    description = Array("only run the job once(for testing)"),
    required = false
  )
  var once: Boolean = false

  @CommandLine.Option(
    names = Array("--latest-only"),
    description = Array("only run the latest schedule(for full job)"),
    required = false
  )
  var latestOnly: Boolean = false

  @CommandLine.Option(
    names = Array("--refresh"),
    description = Array("Refresh data at a specific time"),
    required = false
  )
  var refresh: Boolean = false

  @CommandLine.Option(
    names = Array("--refresh-range-start"),
    description = Array("Refresh data range start"),
    required = false
  )
  var refreshRangeStart: String = _

  @CommandLine.Option(
    names = Array("--refresh-range-end"),
    description = Array("Refresh data range end"),
    required = false
  )
  var refreshRangeEnd: String = _

  @CommandLine.Option(
    names = Array("--from-step"),
    description = Array("(Re-)run from step"),
    required = false
  )
  var fromStep: String = _

  @CommandLine.Option(
    names = Array("--exclude-steps"),
    description = Array("exclude steps"),
    required = false
  )
  var excludeSteps: String = _

  @CommandLine.Option(
    names = Array("--property"),
    description = Array("specify property file location allowed url type [hdfs, file system]"),
    required = false
  )
  var propertyPath: String = _


  @CommandLine.Option(
    names = Array("--env"),
    description = Array("env: local/test/dev/qa/prod"),
    required = false
  )
  var env: String = Environment.LOCAL

  @CommandLine.Option(
    names = Array("--override"),
    description = Array("--override=prop-a=zoo,prop-b=bar"),
    required = false
  )
  var `override`: String = _

  var extraParams: mutable.Map[String, String] = mutable.Map[String, String]()

  val commandStr: mutable.StringBuilder = new mutable.StringBuilder()

  protected def parseExtraOptions(): Unit = {
    try {
      if (!isNullOrEmpty(`override`)) {
        commandStr.append(s"--override=")
        `override`.split(",").map(it => it.split("=")).map(it => it(0) -> it.tail.mkString("=")).toMap
          .foreach { case (key, value) =>
            commandStr.append(s"$key=$value,")
            extraParams += (key -> value)
          }
      }
    } catch {
      case _: Throwable =>
        val message =
          s"""Failed to parse extraParams(parameter after --override) format should be [OPTION=VALUE]
             |current extra options: ${`override`}
             |""".stripMargin
        throw FailedToParseExtraParamsException(message)
    }
  }

  // scalastyle:off
  def formatCommand(): Unit = {
    if (!isNullOrEmpty(defaultStart) && defaultStart.contains(":")) {
      // 2021-09-24 00:00:00 => 20210924000000
      defaultStart = defaultStart.replace("-", "").replace(" ", "").replace(":", "")
    }
    commandStr.append(s"--local=$local \t")
    commandStr.append(s"--release-resource=$releaseResource \t")
    if (!isNullOrEmpty(defaultStart)) commandStr.append(s"--default-start=$defaultStart \t")
    if (!isNullOrEmpty(logDrivenType)) commandStr.append(s"--log-driven-type=$logDrivenType \t")
    if (period > 0) commandStr.append(s"--period=$period \t")
    commandStr.append(s"--once=$once \t")
    commandStr.append(s"--latest-only=$latestOnly \t")
    commandStr.append(s"--env=$env \t")
    commandStr.append(s"--skip-running=$skipRunning \t")
    if (refresh) {
      commandStr.append(s"--refresh=$refresh \t")
      commandStr.append(s"--refresh-range-start=$refreshRangeStart \t")
      commandStr.append(s"--refresh-range-end=$refreshRangeEnd \t")
      commandStr.append(s"--from-step=$fromStep \t")
      commandStr.append(s"exclude-steps=$excludeSteps \t")
    }
    parseExtraOptions()
  }
  // scalastyle:on

  def loggingJobParameters(): Unit = {
    formatCommand()
    println(
      """ .-. .   .    .    .--. .--.   .---  ----- .
        |(    |   |   / \   |   )|   )  |       |   |
        | `-. |---|  /___\  |--' |--'   |---    |   |
        |    )|   | /     \ |  \ |      |       |   |
        | `-' '   ''       `'   `'      '---'   '   '---'
        |""".stripMargin)
    println(
      s"""
         |Java version:                         ${System.getProperty("java.version")}
         |Scala version:                        ${util.Properties.versionNumberString}
         |OS:                                   ${System.getProperty("os.name")}
         |OS arch:                              ${System.getProperty("os.arch")}
         |OS version:                           ${System.getProperty("os.version")}
         |Available processors (cores):         ${Runtime.getRuntime.availableProcessors()}
         |Free memory:                          ${byteCountToDisplaySize(Runtime.getRuntime.freeMemory())}
         |Maximum memory:                       ${byteCountToDisplaySize(Runtime.getRuntime.maxMemory())}
         |Total memory available to JVM:        ${byteCountToDisplaySize(Runtime.getRuntime.totalMemory())}
         |""".stripMargin)
    ETLLogger.info(s"parameters: ${commandStr.toString()}")
    Environment.CURRENT = env
    ETLLogger.info(s"Job profile: ${Environment.CURRENT}")
  }
}

abstract class SingleJobCommand extends CommonCommand {

  @CommandLine.Option(
    names = Array("--name"),
    description = Array("name of the workflow"),
    required = true
  )
  var wfName: String = _

  @CommandLine.Option(
    names = Array("-h", "--help"),
    usageHelp = true,
    description = Array("Sample parameters: --name=your-sql-file-name-without-file-extension --period=1440")
  )
  var helpRequested = false

  override def formatCommand(): Unit = {
    commandStr.append(s"--name=$wfName \t")
    commandStr.append(s"--help=$helpRequested \t")
    super.formatCommand()
  }
}

class ExcelOptions {
  @CommandLine.Option(
    names = Array("-f", "--file"),
    description = Array("config file path"),
    required = true
  )
  var filePath: String = _
}

class SqlFileOptions {
  @CommandLine.Option(
    names = Array("-n", "--names"),
    description = Array("names of the workflow"),
    required = true,
    split = ","
  )
  var wfNames: Array[String] = _
}

abstract class BatchJobCommand extends CommonCommand {

  @ArgGroup(heading = "run batch job by excel%n")
  var excelOptions: ExcelOptions = _

  @ArgGroup(heading = "run batch job by sql file%n", exclusive = false)
  var sqlFileOptions: SqlFileOptions = _

  @CommandLine.Option(
    names = Array("--parallelism"),
    description = Array("control batch job parallelism"),
    required = false
  )
  var parallelism: Int = Runtime.getRuntime.availableProcessors()

  @CommandLine.Option(
    names = Array("-h", "--help"),
    usageHelp = true,
    description = Array("Sample parameters: -f=/path/to/config.xlsx or -n=job1,job2")
  )
  var helpRequested = false

  override def formatCommand(): Unit = {
    if (excelOptions != null) {
      commandStr.append(s"--file=$excelOptions.filePath \t")
    }
    if (sqlFileOptions != null) {
      commandStr.append(s"--names=${sqlFileOptions.wfNames.mkString(",")} \t")
    }
    commandStr.append(s"--parallelism=$parallelism \t")
    super.formatCommand()
  }
}
