package com.github.sharpdata.sharpetl.core.syntax

import fastparse._
import NoWhitespace._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.sharpdata.sharpetl.core.datasource.config.{DataSourceConfig, TransformationDataSourceConfig}
import com.github.sharpdata.sharpetl.core.annotation.AnnotationScanner.{configRegister, defaultConfigType}
import com.github.sharpdata.sharpetl.core.annotation.Annotations.Experimental
import com.github.sharpdata.sharpetl.core.syntax.ParserUtils.{Until, objectMapper, trimSql}


object WorkflowParser {

  def key[_: P]: P0 = P(CharIn("a-z", "A-Z", "0-9", "_", "."))

  def singleLineValue[_: P]: P[String] = Until(newline | End).!

  def multiLineValue[_: P](indent: Int): P[String] =
    (P("|") ~/ newline ~ Until((anyComment ~ otherPart).! | keyValPair(indent) | End))
      .map(value => {
        val replace = multiLineStart(indent)
        value.split("\n").map(_.replace(replace, "")).mkString("\n")
      })

  private def multiLineStart(indent: Int) = {
    "--" + Range.apply(0, indent).map(_ => " ").mkString + "|"
  }

  def newline[_: P]: P0 = P("\n" | "\r\n" | "\r" | "\f")

  def newlines[_: P]: P0 = newline.rep

  def whitespace[_: P]: P0 = P(" " | "\t" | newline)

  def comment[_: P](indent: Int): P0 = P("--" ~ " ".rep(exactly = indent)) ~ !" "

  def anyComment[_: P]: P0 = P("--" ~ " ".rep)

  def otherPart[_: P]: P0 = P("step=" | "source=" | "target=" | "args" ~ newline | "options" ~ newline | "conf" ~ newline)

  def keyValPair[_: P](indent: Int): P[(String, String)] =
    comment(indent) ~ !otherPart ~ P(key.rep(1).!) ~ "=" ~ P(multiLineValue(indent) | singleLineValue)

  def keyValPairs[_: P](indent: Int): P[Seq[(String, String)]] = keyValPair(indent).rep(sep = newlines)

  def stepHeader[_: P]: P0 = P("-- step=")

  def sql[_: P]: P[String] = Until(stepHeader | End)

  def nestedObj[_: P](indent: Int): P[(String, Map[String, String])] = P(
    comment(indent) ~ !P("step=" | "source=" | "target=") ~ P(key.rep(1).!) ~ newlines
      ~ keyValPairs(indent + 1)
  ).map {
    case (obj, kv) => (obj, kv.toMap)
  }

  def options[_: P](indent: Int): P[Map[String, String]] = P(
    comment(indent) ~ !P("step=" | "source=" | "target=") ~ P("options") ~ newlines
      ~ keyValPairs(indent + 1)
  ).map(_.toMap)

  def dataSource[_: P](`type`: String): P[DataSourceConfig] = P(
    s"-- ${`type`}=" ~/ key.rep.! ~ newlines
      ~ keyValPairs(2) ~ newlines
      ~ options(2).?
  ).map {
    case (typeValue, kv, options) =>
      val value = Map(
        "dataSourceType" -> typeValue.toLowerCase
      ) ++ kv.toMap ++ Map("options" -> options.getOrElse(Map()))
      val clazz: Class[DataSourceConfig] = configRegister.getOrElse(typeValue.toLowerCase, defaultConfigType)
      val json = objectMapper.writeValueAsString(value)
      objectMapper.readValue(json, clazz)
  }

  def args[_: P]: P[(String, String)] = P("--" ~ " ".rep(min = 2, max = 3)) ~ !otherPart ~ P(key.rep(1).!) ~ "=" ~ singleLineValue.!

  def transformer[_: P](`type`: String): P[DataSourceConfig] = P(
    s"-- ${`type`}=transformation" ~/ newlines
      ~ args.rep(sep = newlines)
  ).map { kv =>
    val map = kv.toMap
    val value = Map(
      "dataSourceType" -> "transformation",
      "className" -> map.getOrElse("className", ""),
      "methodName" -> map.getOrElse("methodName", ""),
      "transformerType" -> map.getOrElse("transformerType", "")
    ) ++ Map("args" -> map.filterKeys(it => it != "className" && it != "methodName" && it != "transformerType" && it != "dataSourceType").toMap)
    val json = objectMapper.writeValueAsString(value)
    objectMapper.readValue(json, classOf[TransformationDataSourceConfig])
  }

  def steps[_: P]: P[Seq[WorkflowStep]] = step.rep(sep = newlines, min = 1)

  def step[_: P]: P[WorkflowStep] = P(
    newlines ~ stepHeader ~ singleLineValue ~ newlines
      ~ P(transformer("source") | dataSource("source")) ~ newlines
      ~ P(transformer("target") | dataSource("target")) ~ newlines
      ~ keyValPairs(1).? ~ newlines
      ~ nestedObj(1).? ~ newlines
      ~ sql
  ).map {
    // scalastyle:off
    case (step, source, target, kv, opts, sql) =>
      val map = kv.getOrElse(Seq()).toMap
      val workflowStep = new WorkflowStep
      workflowStep.step = step
      workflowStep.source = source
      workflowStep.target = target
      workflowStep.sqlTemplate = trimSql(sql)
      workflowStep.persist = map.getOrElse("persist", null)
      workflowStep.checkPoint = map.getOrElse("checkPoint", null)
      workflowStep.writeMode = map.getOrElse("writeMode", null)
      workflowStep.skipFollowStepWhenEmpty = map.getOrElse("skipFollowStepWhenEmpty", null) //TODO: drop this later
      workflowStep.conf = opts.getOrElse(("", Map[String, String]()))._2
      workflowStep
    //      WorkflowStep(step, source, target, sql.map(_.trim),
    //        map.getOrElse("persist", null), map.getOrElse("checkpoint", null),
    //        map.getOrElse("writeMode", null),
    //        opts.getOrElse(("", Map[String, String]()))._2)
    // scalastyle:on
  }


  def workflow[_: P]: P[Workflow]
  = P(
    Start
      ~ whitespace.rep
      ~ "-- workflow" ~/ "=" ~/ singleLineValue ~ newlines
      ~ keyValPairs(2) ~ newlines
      ~ nestedObj(2).rep(sep = newlines) ~ newlines
      ~ steps
      ~ End
  ).map { case (name, kv, objs, steps) =>
    val value = Map(
      "name" -> name
    ) ++ kv.toMap ++ objs.map {
      case (obj, kv) => obj -> kv
    }
    val json = objectMapper.writeValueAsString(value)
    val wf = objectMapper.readValue(json, classOf[Workflow])
    wf.steps = steps.toList
    wf
  }

  @Experimental(message = "experimental workflow parser", since = "1.0.0")
  def parseWorkflow(text: String): WFParseResult = {
    parse(text, workflow(_)) match {
      case Parsed.Success(value, _) => WFParseSuccess(value)
      case Parsed.Failure(label, failIndex, extra) => WFParseFail(Parsed.Failure(label, failIndex, extra))
    }
  }
}

private object ParserUtils {

  val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
  objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, false)

  // scalastyle:off
  def Until(p: => P[_])(implicit ctx: P[_]): P[String] = {
    (!p ~ AnyChar).rep.! ~ &(p)
  }
  // scalastyle:on

  def trimSql(sql: String): String = {
    val trim = sql.trim
    if (trim.endsWith(";")) {
      trim.slice(0, trim.length - 1)
    } else {
      trim
    }
  }
}

sealed trait WFParseResult

case class WFParseSuccess(wf: Workflow) extends WFParseResult

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
}

