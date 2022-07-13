package com.github.sharpdata.sharpetl.spark.test

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

case class DatasetSchemaMismatch(smth: String) extends Exception(smth)

case class DatasetContentMismatch(smth: String) extends Exception(smth)

case class DatasetCountMismatch(smth: String) extends Exception(smth)

object DatasetComparerLike {

  def naiveEquality[T](o1: T, o2: T): Boolean = {
    o1.equals(o2)
  }

}

trait DatasetComparer {

  private def betterSchemaMismatchMessage[T](actualDS: Dataset[T], expectedDS: Dataset[T]): String = {
    "\nActual Schema Field | Expected Schema Field\n" + actualDS.schema
      .zipAll(
        expectedDS.schema,
        "",
        ""
      )
      .map {
        case (sf1, sf2) if sf1 == sf2 =>
          (s"$sf1 | $sf2")
        case ("", sf2) =>
          (s"MISSING | $sf2")
        case (sf1, "") =>
          (s"$sf1 | MISSING")
        case (sf1, sf2) =>
          (s"$sf1 | $sf2")
      }
      .mkString("\n")
  }

  private def betterContentMismatchMessage[T](a: Array[T], e: Array[T], truncate: Int): String = {
    s"""
       |Actual Content: ${a.take(truncate).mkString("Array(", ", ", ")")}
       |Expected Content: ${e.take(truncate).mkString("Array(", ", ", ")")}
       |""".stripMargin
  }

  /**
   * Raises an error unless `actualDS` and `expectedDS` are equal
   */
  def assertSmallDatasetEquality[T](actualDS: Dataset[T],
                                    expectedDS: Dataset[T],
                                    ignoreNullable: Boolean = false,
                                    ignoreColumnNames: Boolean = false,
                                    orderedComparison: Boolean = true,
                                    truncate: Int = 500): Unit = {
    if (!SchemaComparer.equals(actualDS.schema, expectedDS.schema, ignoreNullable, ignoreColumnNames)) {
      throw DatasetSchemaMismatch(
        betterSchemaMismatchMessage(actualDS, expectedDS)
      )
    }
    if (orderedComparison) {
      val a = actualDS.collect()
      val e = expectedDS.collect()
      if (!a.sameElements(e)) {
        throw DatasetContentMismatch(betterContentMismatchMessage(a, e, truncate))
      }
    } else {
      val a = defaultSortDataset(actualDS).collect()
      val e = defaultSortDataset(expectedDS).collect()
      if (!a.sameElements(e)) {
        throw DatasetContentMismatch(betterContentMismatchMessage(a, e, truncate))
      }
    }
  }

  def defaultSortDataset[T](ds: Dataset[T]): Dataset[T] = {
    val colNames = ds.columns
    val cols = colNames.map(col)
    ds.sort(cols: _*)
  }

  def sortPreciseColumns[T](ds: Dataset[T]): Dataset[T] = {
    val colNames = ds.dtypes
      .withFilter { dtype =>
        !(Seq("DoubleType", "DecimalType", "FloatType").contains(dtype._2))
      }
      .map(_._1)
    val cols = colNames.map(col)
    ds.sort(cols: _*)
  }
}