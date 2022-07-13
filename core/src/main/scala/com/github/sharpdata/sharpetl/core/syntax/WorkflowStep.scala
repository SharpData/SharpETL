package com.github.sharpdata.sharpetl.core.syntax

import com.github.sharpdata.sharpetl.core.util.Constants.{BooleanString, WriteMode}
import com.google.common.base.Strings.isNullOrEmpty
import com.github.sharpdata.sharpetl.core.annotation.Annotations.Evolving
import com.github.sharpdata.sharpetl.core.datasource.config.DataSourceConfig
import com.github.sharpdata.sharpetl.core.util.Constants.BooleanString
import com.github.sharpdata.sharpetl.core.util.Constants.Separator.ENTER
import com.github.sharpdata.sharpetl.core.util.StringUtil

import scala.beans.BeanProperty

@Evolving(since = "1.0.0")
class WorkflowStep extends Formatable {

  @BeanProperty
  var step: String = _

  var source: DataSourceConfig = _

  var target: DataSourceConfig = _

  @BeanProperty
  var sql: String = _

  @BeanProperty
  var sqlTemplate: String = _

  // repartition creates new partitions and does a full shuffle. default none.
  @BeanProperty
  var repartition: String = _

  // uses existing partitions to minimize the amount of data that's shuffled. default none.
  @BeanProperty
  var coalesce: String = _

  /**
   * 是否需要缓存本步骤执行结果 需指定缓存级别 可选级别如下：
   * NONE
   * DISK_ONLY
   * DISK_ONLY_2
   * MEMORY_ONLY
   * MEMORY_ONLY_2
   * MEMORY_ONLY_SER
   * MEMORY_ONLY_SER_2
   * MEMORY_AND_DISK
   * MEMORY_AND_DISK_2
   * MEMORY_AND_DISK_SER
   * MEMORY_AND_DISK_SER_2
   */
  @BeanProperty
  var persist: String = "MEMORY_AND_DISK"

  // 是否需要对本步骤执行结果保存local checkpoint，默认不进行checkpoint
  @BeanProperty
  var checkPoint: String = BooleanString.FALSE

  /**
   * 输出模式，可选类型参照 [[WriteMode]]
   */
  @BeanProperty
  var writeMode: String = _

  // 是否需要在获取到空数据时报错
  @BeanProperty
  var throwExceptionIfEmpty: String = BooleanString.FALSE

  // 是否使用目标表的schema，简化source表没有显示定义schema时的配置
  @BeanProperty
  var isUseTargetSchema: String = BooleanString.FALSE

  // 是否跳过后面步骤当数据或文件为空
  @BeanProperty
  var skipFollowStepWhenEmpty: String = BooleanString.FALSE

  def getSourceConfig[T <: DataSourceConfig]: T = source.asInstanceOf[T]

  def setSourceConfig(sourceConfig: DataSourceConfig): Unit = {
    this.source = sourceConfig
  }

  def getTargetConfig[T <: DataSourceConfig]: T = target.asInstanceOf[T]

  def setTargetConfig(targetConfig: DataSourceConfig): Unit = {
    this.target = targetConfig
  }

  @BeanProperty
  var conf: Map[String, String] = Map[String, String]()


  override def toString: String = {
    val builder = new StringBuilder()
    builder.append(s"-- step=$step$ENTER")
    builder.append(s"-- source=${source.dataSourceType}$ENTER")
    if (!isNullOrEmpty(source.toString.trim)) builder.append(s"${source.toString.trim}$ENTER")
    builder.append(s"-- target=${target.dataSourceType}$ENTER")
    if (!isNullOrEmpty(target.toString.trim)) builder.append(s"${target.toString.trim}$ENTER")
    if (!StringUtil.isNullOrEmpty(repartition)) builder.append(s"-- repartition=$repartition$ENTER")
    if (!StringUtil.isNullOrEmpty(coalesce)) builder.append(s"-- coalesce=$coalesce$ENTER")
    if (!StringUtil.isNullOrEmpty(persist) && persist != "MEMORY_AND_DISK") builder.append(s"-- writeMode=$persist$ENTER")
    if (!StringUtil.isNullOrEmpty(checkPoint) && checkPoint != "false") builder.append(s"-- checkPoint=$checkPoint$ENTER")
    buildOptionsString(builder)
    buildFileOptionString(builder)
    builder.toString()
  }

  // scalastyle:off
  def buildOptionsString(builder: StringBuilder): Unit = {
    if (!StringUtil.isNullOrEmpty(writeMode)) builder.append(s"-- writeMode=$writeMode$ENTER")
    if (!StringUtil.isNullOrEmpty(throwExceptionIfEmpty) && throwExceptionIfEmpty == BooleanString.TRUE) {
      builder.append(s"-- throwExceptionIfEmpty=$throwExceptionIfEmpty$ENTER")
    }
    if (!StringUtil.isNullOrEmpty(isUseTargetSchema) && isUseTargetSchema == BooleanString.TRUE) {
      builder.append(s"-- isUseTargetSchema=$isUseTargetSchema$ENTER")
    }
    if (conf.nonEmpty) {
      builder.append(s"-- conf$ENTER")
      conf.foreach { case (key, value) => builder.append(s"--  $key=$value$ENTER") }
    }
    if (!StringUtil.isNullOrEmpty(sql)) {
      builder.append(s"${sql.trim};$ENTER")
    } else {
      if (!StringUtil.isNullOrEmpty(sqlTemplate)) builder.append(s"${sqlTemplate.trim};$ENTER")
    }
  }
  // scalastyle:on

  def buildFileOptionString(builder: StringBuilder): Unit = {
    if (!StringUtil.isNullOrEmpty(skipFollowStepWhenEmpty) && skipFollowStepWhenEmpty == BooleanString.TRUE) {
      builder.append(s"-- skipFollowStepWhenEmpty=$skipFollowStepWhenEmpty$ENTER")
    }

  }
}
