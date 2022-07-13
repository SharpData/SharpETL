package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.util.Constants.Separator.ENTER
import com.github.sharpdata.sharpetl.core.util.{JdbcDefaultOptions, StringUtil}

import scala.beans.BeanProperty

class DBDataSourceConfig extends DataSourceConfig {
  @BeanProperty
  var connectionName: String = _

  @BeanProperty
  var dbName: String = _

  @BeanProperty
  var tableName: String = _

  @BeanProperty
  var batchSize: String = 1024.toString

  // The maximum number of partitions that can be used for parallelism in table reading and writing.
  @BeanProperty
  var numPartitions: String = _

  // They describe how to partition the table when reading in parallel from multiple workers.
  @BeanProperty
  var partitionColumn: String = _

  // lowerBound is just used to decide the partition stride, not for filtering the rows in table.
  @BeanProperty
  var lowerBound: String = _

  // upperBound is just used to decide the partition stride, not for filtering the rows in table.
  @BeanProperty
  var upperBound: String = _

  // primary keys, only used by ElasticSearch
  @BeanProperty
  var primaryKeys: String = _

  // use transaction or not, false by default
  @BeanProperty
  var transaction: String = false.toString

  // scalastyle:off
  override def toString: String = {
    val builder = new StringBuilder()

    if (!StringUtil.isNullOrEmpty(connectionName)) builder.append(s"--  connectionName=$connectionName$ENTER")
    if (!StringUtil.isNullOrEmpty(dbName)) builder.append(s"--  dbName=$dbName$ENTER")
    if (!StringUtil.isNullOrEmpty(tableName)) builder.append(s"--  tableName=$tableName$ENTER")
    if (!StringUtil.isNullOrEmpty(batchSize) && batchSize != JdbcDefaultOptions.BATCH_SIZE.toString) builder.append(s"--  batchSize=$batchSize$ENTER")
    if (!StringUtil.isNullOrEmpty(numPartitions)) builder.append(s"--  numPartitions=$numPartitions$ENTER")
    if (!StringUtil.isNullOrEmpty(partitionColumn)) builder.append(s"--  partitionColumn=$partitionColumn$ENTER")
    if (!StringUtil.isNullOrEmpty(lowerBound)) builder.append(s"--  lowerBound=$lowerBound$ENTER")
    if (!StringUtil.isNullOrEmpty(upperBound)) builder.append(s"--  upperBound=$upperBound$ENTER")
    if (!StringUtil.isNullOrEmpty(primaryKeys)) builder.append(s"--  primaryKeys=$primaryKeys$ENTER")
    if (!transaction.equalsIgnoreCase(false.toString)) builder.append(s"--  transaction=$transaction$ENTER")
    builder.append(optionsToString)
    builder.toString
  }
  // scalastyle:on
}
