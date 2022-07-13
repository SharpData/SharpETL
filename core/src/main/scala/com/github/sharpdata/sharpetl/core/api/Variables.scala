package com.github.sharpdata.sharpetl.core.api

import com.github.sharpdata.sharpetl.core.annotation.Annotations.Stable

@Stable(since = "1.0.0")
final case class Variables(private val value: collection.mutable.Map[String, String]) {
  def put(k: String, v: String): Unit = value.put(k, v)

  def filter(function: ((String, String)) => Boolean): Variables = Variables(value.filter(function))

  def apply(key: String): String = value(key)

  def foreach[U](f: (String, String) => U): Unit = value.foreach {
    case (k, v) => f(k, v)
  }

  def contains(key: String): Boolean = {
    value.contains(key)
  }

  def getOrElse(key: String, default: () => String): String = {
    value.getOrElse(key, default())
  }

  def getOrElse(key: String, default: String): String = {
    value.getOrElse(key, default)
  }

  def ++(xs: Map[String, String]): Variables = Variables(value ++ xs) // scalastyle:off

  def +=(kv: (String, String)): Variables = {
    value += kv
    this
  }
}

object Variables {
  def empty: Variables = Variables(collection.mutable.Map())
}
