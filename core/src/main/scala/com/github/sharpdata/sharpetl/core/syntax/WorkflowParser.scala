package com.github.sharpdata.sharpetl.core.syntax

import fastparse._
import NoWhitespace._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.sharpdata.sharpetl.core.datasource.config.{DataSourceConfig, TransformationDataSourceConfig}
import com.github.sharpdata.sharpetl.core.annotation.AnnotationScanner.{configRegister, defaultConfigType, tempConfig}
import com.github.sharpdata.sharpetl.core.annotation.Annotations.Experimental
import com.github.sharpdata.sharpetl.core.syntax.ParserUtils.{Until, objectMapper, trimSql}


object WorkflowParser {

  def key[$: P]: P0 = P(CharIn("a-z", "A-Z", "0-9", "_", "."))

  def singleLineValue[$: P]: P[String] = Until(newline | End).!

  def multiLineValue[$: P](indent: Int): P[String] =
    (P("|") ~/ newline ~ Until((anyComment ~ key) | End))
      .map(value => {
        val replace = multiLineStart(indent)
        value.split("\n").map(_.replace(replace, "")).mkString("\n")
      })

  private def multiLineStart(indent: Int) = {
    "--" + Range.apply(0, indent).map(_ => " ").mkString + "|"
  }

  def newline[$: P]: P0 = P("\n" | "\r\n" | "\r" | "\f")

  def newlines[$: P]: P0 = newline.rep

  def whitespace[$: P]: P0 = P(" " | "\t" | newline)

  def comment[$: P](indent: Int): P0 = P("--" ~ " ".rep(exactly = indent)) ~ !" "

  def anyComment[$: P]: P0 = P("--" ~ " ".rep)

  def otherPart[$: P]: P0 = P("step=" | "source=" | "target=" | "args" ~ newline | "options" ~ newline | "conf" ~ newline | "loopOver=")

  def keyValPair[$: P](indent: Int): P[(String, String)] =
    comment(indent) ~ !otherPart ~ P(key.rep(1).!) ~ "=" ~ P(multiLineValue(indent) | singleLineValue)

  def keyValPairs[$: P](indent: Int): P[Seq[(String, String)]] = keyValPair(indent).rep(sep = newlines)

  def stepHeader[$: P]: P0 = P("-- step=")

  def sql[$: P]: P[String] = Until(stepHeader | End)

  def nestedObj[$: P](objName: String, indent: Int): P[Map[String, String]] = P(
    comment(indent) ~ P(objName) ~ newlines
      ~ keyValPairs(indent + 1)
  ).map(_.toMap)

  def notifies[$: P](indent: Int): P[Seq[Map[String, String]]] = notify(indent).rep(sep = newlines)

  def notify[$: P](indent: Int): P[Map[String, String]] = nestedObj("notify", indent)

  def options[$: P](indent: Int): P[Map[String, String]] = nestedObj("options", indent)

  def conf[$: P](indent: Int): P[Map[String, String]] = nestedObj("conf", indent)

  def loopOver[$: P]: P[String] = P("-- loopOver=") ~ singleLineValue.!

  def dataSource[$: P](`type`: String): P[DataSourceConfig] = P(
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

  def args[$: P]: P[(String, String)] = P("--" ~ " ".rep(min = 2, max = 3)) ~ !otherPart ~ P(key.rep(1).!) ~ "=" ~ singleLineValue.!

  def transformer[$: P](`type`: String): P[DataSourceConfig] = P(
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

  def steps[$: P]: P[Seq[WorkflowStep]] = step.rep(sep = newlines, min = 1)

  def step[$: P]: P[WorkflowStep] = P(
    newlines ~ stepHeader ~ singleLineValue ~ newlines
      ~ P(transformer("source") | dataSource("source")).? ~ newlines
      ~ P(transformer("target") | dataSource("target")) ~ newlines
      ~ keyValPairs(1).? ~ newlines
      ~ conf(1).? ~ newlines
      ~ loopOver.? ~ newlines
      ~ sql
  ).map {
    // scalastyle:off
    case (step, sourceOptional, target, kv, conf, loopOverOptional, sql) =>
      val map = kv.getOrElse(Seq()).toMap
      val workflowStep = new WorkflowStep
      workflowStep.step = step
      workflowStep.source = sourceOptional.getOrElse(tempConfig)
      workflowStep.target = target
      workflowStep.sqlTemplate = trimSql(sql)
      workflowStep.persist = map.getOrElse("persist", null)
      workflowStep.checkPoint = map.getOrElse("checkPoint", null)
      workflowStep.writeMode = map.getOrElse("writeMode", null)
      workflowStep.skipFollowStepWhenEmpty = map.getOrElse("skipFollowStepWhenEmpty", null) //TODO: drop this later
      workflowStep.conf = conf.getOrElse(Map())
      workflowStep.loopOver = loopOverOptional.orNull
      workflowStep
    //      WorkflowStep(step, source, target, sql.map(_.trim),
    //        map.getOrElse("persist", null), map.getOrElse("checkpoint", null),
    //        map.getOrElse("writeMode", null),
    //        opts.getOrElse(("", Map[String, String]()))._2)
    // scalastyle:on
  }


  def workflow[$: P]: P[Workflow]
  = P(
    Start
      ~ whitespace.rep
      ~ "-- workflow" ~/ "=" ~/ singleLineValue ~ newlines
      ~ keyValPairs(2) ~/ newlines
      ~ options(2).? ~/ newlines
      ~ notifies(2).? ~/ newlines
      ~ steps
      ~ End
  ).map { case (name, kv, options, notifies, steps) =>
    val value = kv.toMap + ("name" -> name) + ("options" -> options.getOrElse(Map())) + ("notifies" -> notifies.getOrElse(Seq()))
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
