package com.github.sharpdata.sharpetl.core.util

import com.github.sharpdata.sharpetl.core.annotation.Annotations.Private

import scala.util.control.NonFatal

@Private
sealed trait Try[+T] extends Product with Serializable {
  def getOrElse[U >: T](default: => U): U

  def get: T

  def isSuccess(): Boolean

  def isFailure(): Boolean

  def isSkipped(): Boolean
}

@Private
object Try {
  def apply[T](f: T => T, t: T): Try[T] =
    try Success(f(t)) catch {
      case NonFatal(e) => Failure(t, e)
    }
}

@Private
final case class Success[+T](result: T) extends Try[T] {
  override def getOrElse[U >: T](default: => U): U = result

  override def isSuccess(): Boolean = true

  override def isFailure(): Boolean = false

  override def isSkipped(): Boolean = false

  override def get: T = result
}

@Private
final case class Failure[+T](result: T, e: Throwable) extends Try[T] {
  override def getOrElse[U >: T](default: => U): U = default

  override def isSuccess(): Boolean = false

  override def isFailure(): Boolean = true

  override def isSkipped(): Boolean = false

  override def get: T = result
}

@Private
final case class Skipped[+T](result: T) extends Try[T] {
  override def getOrElse[U >: T](default: => U): U = default

  override def isSuccess(): Boolean = false

  override def isFailure(): Boolean = false

  override def isSkipped(): Boolean = true

  override def get: T = result
}
