package com.github.sharpdata.sharpetl.flink

import com.github.sharpdata.sharpetl.flink.cli.Command
import picocli.CommandLine


object Entrypoint {
  val errorHandler: CommandLine.IExecutionExceptionHandler =
    new CommandLine.IExecutionExceptionHandler() {
      def handleExecutionException(ex: Exception, commandLine: CommandLine, parseResult: CommandLine.ParseResult): Int = {
        println("Failed to execute job, exiting with error: " + ex.getMessage)
        ex.printStackTrace()
        commandLine.getCommandSpec.exitCodeOnExecutionException
      }
    }

  def main(args: Array[String]): Unit = {
    val code = new CommandLine(new Command()).setExecutionExceptionHandler(errorHandler).execute(
      args: _*
    )
    if (!succeed(code)) {
      println("Failed to execute job, exiting with code " + code)
      System.exit(code)
    }
  }

  private def succeed(code: Int) = {
    code == 0
  }
}
