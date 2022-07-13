package com.github.sharpdata.sharpetl.core

import com.github.sharpdata.sharpetl.core.datasource.config._
import com.github.sharpdata.sharpetl.core.syntax.Formatable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.beans.BeanProperty

class FormatableSpec extends AnyFlatSpec with should.Matchers {

  it should "format config content" in {
    class JobConfig extends Formatable {
      @BeanProperty var inputConfig = new TextFileDataSourceConfig
      @BeanProperty var outputConfig = new CSVDataSourceConfig
    }

    val config = new JobConfig()
    config.toString should be(
      """-- inputConfig
        |--  encoding=UTF-8
        |--  codecExtension=
        |--  decompress=false
        |--  strictColumnNum=false
        |--  fileNamePattern=.*
        |--  deleteSource=false
        |-- outputConfig
        |--  inferSchema=true
        |--  encoding=UTF-8
        |--  sep=,
        |--  header=true
        |--  quote="
        |--  escape="
        |--  multiLine=false
        |--  ignoreTrailingWhiteSpace=false
        |--  selectExpr=*
        |--  parseTimeFromFileNameRegex=
        |--  parseTimeFormatPattern=
        |--  parseTimeColumnName=parsedTime
        |--  fileNamePattern=.*
        |--  deleteSource=false""".stripMargin)

  }
}
