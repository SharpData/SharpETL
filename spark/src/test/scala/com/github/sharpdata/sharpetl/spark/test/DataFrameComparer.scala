package com.github.sharpdata.sharpetl.spark.test

import org.apache.spark.sql.DataFrame

trait DataFrameComparer extends DatasetComparer {

  /**
   * Raises an error unless `actualDF` and `expectedDF` are equal
   */
  def assertSmallDataFrameEquality(actualDF: DataFrame,
                                   expectedDF: DataFrame,
                                   ignoreNullable: Boolean = false,
                                   ignoreColumnNames: Boolean = false,
                                   orderedComparison: Boolean = true,
                                   truncate: Int = 500): Unit = {
    assertSmallDatasetEquality(
      actualDF,
      expectedDF,
      ignoreNullable,
      ignoreColumnNames,
      orderedComparison,
      truncate
    )
  }
}
