package com.github.sharpdata.sharpetl.flink

import com.github.sharpdata.sharpetl.flink.cli.Command
import picocli.CommandLine


object Entrypoint {
  val errorHandler: CommandLine.IExecutionExceptionHandler =
    new CommandLine.IExecutionExceptionHandler() {
      def handleExecutionException(ex: Exception, commandLine: CommandLine, parseResult: CommandLine.ParseResult): Int = {
        ex.printStackTrace()
        commandLine.getCommandSpec.exitCodeOnExecutionException
      }
    }

  def main(args: Array[String]): Unit = {
    val code = new CommandLine(new Command()).setExecutionExceptionHandler(errorHandler).execute(
      args: _*
    )
    if (!succeed(code)) {
      System.exit(code)
    }
  }

  private def succeed(code: Int) = {
    code == 0
  }
}
