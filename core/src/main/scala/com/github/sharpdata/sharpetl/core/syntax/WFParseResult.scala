package com.github.sharpdata.sharpetl.core.syntax

import com.github.sharpdata.sharpetl.core.exception.Exception.WorkFlowSyntaxException
import fastparse.{IndexedParserInput, Parsed, ParserInput}


sealed trait WFParseResult {
  def isSuccess: Boolean

  def get: Workflow
}

case class WFParseSuccess(wf: Workflow) extends WFParseResult {
  override def isSuccess: Boolean = true

  override def get: Workflow = wf
}

case class WFParseFail(parsed: Parsed.Failure) extends WFParseResult {
  override def toString: String = {
    parsed match {
      case Parsed.Failure(label, failIndex, extra) =>
        val trace = extra.trace()
        val last = trace.stack.last
        val input: ParserInput = trace.input
        val pair = input.prettyIndex(last._2).split(":")
        val row = pair.head.toInt
        val col = pair.tail.head.toInt

        val line = {
          val lines = trace.input.asInstanceOf[IndexedParserInput].data.split("\n")
          if (lines.size <= row) {
            lines.last
          } else {
            lines(row - 1)
          }
        }
        val offending =
          s"${row.toString map { _ => ' ' }}|${" " * (col - 1)}^"
        s"""$row:$col: error: ${description(input, trace.stack, failIndex)}
           |$row|$line
           |$offending""".stripMargin
    }
  }

  def description(input: ParserInput, stack: List[(String, Int)], index: Int): String = {
    s"""Expected parse by `${stack.reverse.head._1}` at ${input.prettyIndex(stack.reverse.head._2)}, but found ${formatTrailing(input, index)}.
       |Parse stack is ${formatStack(input, stack)}""".stripMargin
  }

  def formatStack(input: ParserInput, stack: List[(String, Int)]): String = {
    stack.map { case (s, i) => s"$s:${input.prettyIndex(i)}" }.mkString(" / ")
  }

  def formatTrailing(input: ParserInput, index: Int): String = {
    fastparse.internal.Util.literalize(input.slice(index, index + 10))
  }

  override def isSuccess: Boolean = false

  override def get: Workflow = throw WorkFlowSyntaxException(this.toString)
}

