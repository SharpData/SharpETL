package com.github.sharpdata.sharpetl.modeling

object Exception {
  final case class TargetTableNotExistException(msg: String) extends RuntimeException(msg)

  final case class TooManyTargetTableException(msg: String) extends RuntimeException(msg)

  final case class UnsupportedPartitionPatternException(message: String) extends Exception(message)

  final case class TableConfigAndColumnConfigNoMatchException(msg: String) extends RuntimeException(msg)

  final case class TableConfigHasDuplicateSourceAndTargetTableException(msg: String) extends RuntimeException(msg)
}
