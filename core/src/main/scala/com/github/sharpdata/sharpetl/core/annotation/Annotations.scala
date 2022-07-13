package com.github.sharpdata.sharpetl.core.annotation

object Annotations {
  final case class Stable(since: String) extends scala.annotation.StaticAnnotation

  final case class Evolving(since: String) extends scala.annotation.StaticAnnotation

  final case class Experimental(message: String, since: String) extends scala.annotation.StaticAnnotation

  final case class Private() extends scala.annotation.StaticAnnotation
}
