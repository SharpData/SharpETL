package com.github.sharpdata.sharpetl.core.datasource.config

import com.github.sharpdata.sharpetl.core.util.Constants.BooleanString

import scala.beans.BeanProperty

class FileDataSourceConfig extends DataSourceConfig with Serializable {

  // 文件类型系统的配置前缀，一般为系统缩写
  @BeanProperty
  var configPrefix: String = _

  // 文件目录（此处配置会覆盖 application.properties 中的 ....ftp.dir、....hdfs.dir ）
  @BeanProperty
  var fileDir: String = _

  // 文件名称正则 三段式（前缀、文件名、后缀）
  @BeanProperty
  var fileNamePattern: String = ".*"

  // 文件过滤函数
  @BeanProperty
  var fileFilterFunc: String = _

  // 文件具体路径（无需配置，任务运行过程中自动查找）
  @BeanProperty
  var filePath: String = _

  /**
   * file path list, split by ',', if [[FileDataSourceConfig.filePaths]] was defined,
   * [[FileDataSourceConfig.fileNamePattern]] will be ignored.
   */
  @BeanProperty
  var filePaths: String = _

  // 加载完成后是否删除源数据，默认不删
  @BeanProperty
  var deleteSource: String = BooleanString.FALSE
}
