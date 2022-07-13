package com.github.sharpdata.sharpetl.core.quality

import com.github.sharpdata.sharpetl.core.exception.Exception.BadDataQualityCheckRuleException

import scala.io.Source

object QualityCheckRuleConfig extends Serializable {
  def readQualityCheckRules(): Map[String, QualityCheckRule] = {
    val yamlString = Source.fromInputStream(getClass.getResourceAsStream("/quality-check.yaml")).getLines.mkString("\n")

    import cats.syntax.either._
    import io.circe._
    import io.circe.generic.auto._
    import io.circe.yaml.parser

    parser.parse(yamlString)
      .leftMap(err => err: Error)
      .flatMap(_.as[List[QualityCheckRule]])
      .valueOr(it => throw BadDataQualityCheckRuleException(
        s"Data quality check rules not valid, please check config file: quality-check.yaml, error: ${it.getMessage}")
      )
      .groupBy(_.dataCheckType)
      .mapValues(_.head)
      .toMap
  }
}
