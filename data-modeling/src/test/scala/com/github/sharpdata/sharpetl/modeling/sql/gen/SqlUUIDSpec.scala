package com.github.sharpdata.sharpetl.modeling.sql.gen

import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.IOUtil
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class SqlUUIDSpec extends AnyFlatSpec with should.Matchers {
  val uuidRegex = "[a-f\\d]{8,32}"

  def readExpectConfig(path: String): String = {
    IOUtil
      .readLinesFromResource(path)
      .map(_.replaceAll(uuidRegex, "uuid"))
      .mkString("\n")
      .trim
  }


  def toActualConfig(steps: List[WorkflowStep]): String = {
    steps
      .map(_.toString.replaceAll(uuidRegex, "uuid"))
      .mkString("\n")
      .trim
  }
}
