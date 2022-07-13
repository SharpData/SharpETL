package com.github.sharpdata.sharpetl.core.util

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

final case class Memo1[A, B](f: A => B) extends (A => B) {
  private[this] val cache = new ConcurrentHashMap[A, B].asScala

  def apply(a: A): B = cache getOrElseUpdate(a, {
    f(a)
  })
}

final case class Memo2[A, B, C](f: (A, B) => C) extends ((A, B) => C) {
  private[this] val cache = new ConcurrentHashMap[(A, B), C]().asScala

  def apply(a: A, b: B): C = cache getOrElseUpdate((a, b), {
    f(a, b)
  })
}
