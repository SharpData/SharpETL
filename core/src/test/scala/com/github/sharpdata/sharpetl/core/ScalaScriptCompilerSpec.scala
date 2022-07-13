package com.github.sharpdata.sharpetl.core

import com.github.sharpdata.sharpetl.core.util.ScalaScriptCompiler
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

trait TestTrait {
  def apply(args: Map[String, String]): String
}

class ScalaScriptCompilerSpec extends AnyFlatSpec with should.Matchers {
  it should s"compile object scala script" in {
    ScalaScriptCompiler.compileTransformer(
      """
        |object TestObject extends com.github.sharpdata.sharpetl.core.TestTrait{
        | def apply(args: Map[String, String]): String = {
        |   args.values.reduce(_+_)
        | }
        |}
        |""".stripMargin).asInstanceOf[TestTrait](Map("a" -> "A", "b" -> "B")) should be("AB")
  }


  it should s"compile object scala script with define" in {
    ScalaScriptCompiler.compileTransformer(
      """
        |object TestObject extends com.github.sharpdata.sharpetl.core.TestTrait{
        | def apply(args: Map[String, String]): String = {
        |   args.values.reduce((left, right) => ABC(left).toString + ABC(right).toString)
        | }
        |}
        |
        |final case class ABC(value: String)
        |""".stripMargin).asInstanceOf[TestTrait](Map("a" -> "A", "b" -> "B")) should be("ABC(A)ABC(B)")
  }
}
