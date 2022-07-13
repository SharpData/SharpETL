package com.github.sharpdata.sharpetl.datasource.es

import com.github.sharpdata.sharpetl.core.api.Variables
import com.github.sharpdata.sharpetl.core.datasource.Sink
import com.github.sharpdata.sharpetl.core.annotation._
import com.github.sharpdata.sharpetl.core.datasource.config.DBDataSourceConfig
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark.sql.EsSparkSQL

@sink(types = Array("es"))
class EsDataSource extends Sink[DataFrame] {
  override def sink(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = {
    val targetConfig = step.target.asInstanceOf[DBDataSourceConfig]
    val esConfig = EsConfig.buildConfig(targetConfig.getPrimaryKeys).++(targetConfig.getOptions)
    EsSparkSQL.saveToEs(
      srdd = df,
      resource = targetConfig.getTableName,
      cfg = esConfig
    )
  }
}
