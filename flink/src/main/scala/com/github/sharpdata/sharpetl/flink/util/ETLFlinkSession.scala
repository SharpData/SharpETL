package com.github.sharpdata.sharpetl.flink.util

import com.github.sharpdata.sharpetl.core.quality.QualityCheckRule
import com.github.sharpdata.sharpetl.core.repository.QualityCheckAccessor
import com.github.sharpdata.sharpetl.core.util.Constants.ETLDatabaseType.FLINK_SHARP_ETL
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, ETLLogger}
import com.github.sharpdata.sharpetl.flink.job.FlinkWorkflowInterpreter
import com.github.sharpdata.sharpetl.flink.udf.CollectWsUDF
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object ETLFlinkSession {
  var local = false
  var wfName = "default"
  private var autoCloseSession: Boolean = true

  val batchSettings: EnvironmentSettings = EnvironmentSettings.newInstance
    .withConfiguration(getConf())
    //.inStreamingMode
    .inBatchMode
    .build()

  val batchEnv: TableEnvironment = TableEnvironment.create(batchSettings)

  private def getConf(): Configuration = {
    val conf = new Configuration
    ETLConfig
      .getFlinkProperties(wfName)
      .foreach {
        case (key, value) =>
          conf.setString(key, value)
          println(s"[Set flink config]: $key=$value")
      }
    conf
  }

  def initUdf(session: TableEnvironment): Unit = {
    session.createTemporarySystemFunction("collect_ws", classOf[CollectWsUDF])
  }

  def getFlinkInterpreter(local: Boolean,
                          wfName: String,
                          autoCloseSession: Boolean,
                          etlDatabaseType: String,
                          dataQualityCheckRules: Map[String, QualityCheckRule])
  : FlinkWorkflowInterpreter = {
    ETLFlinkSession.local = local
    ETLFlinkSession.wfName = wfName
    ETLFlinkSession.autoCloseSession = autoCloseSession
    val session = ETLFlinkSession.batchEnv
    initUdf(session)
    createCatalogIfNeed(etlDatabaseType, session)
    new FlinkWorkflowInterpreter(session, dataQualityCheckRules, QualityCheckAccessor.getInstance(etlDatabaseType))
  }

  //  def release(spark: SparkSession): Unit = {
  //    if (spark != null && autoCloseSession) {
  //      spark.stop()
  //    }
  //  }

  def createCatalogIfNeed(etlDatabaseType: String, session: TableEnvironment): Unit = {
    if (etlDatabaseType == FLINK_SHARP_ETL) {
      val catalogName = ETLConfig.getProperty("flyway.catalog")
      val catalog = session.getCatalog(catalogName)
      if (!catalog.isPresent) {
        if (local) {
          ETLLogger.info(s"catalog $catalogName not found, create it")
          session.executeSql(
            s"""
               |CREATE CATALOG $catalogName
               |WITH (
               |  'type' = 'paimon',
               |  'warehouse' = '${ETLConfig.getProperty("flyway.warehouse")}',
               |  'fs.oss.endpoint' = '${ETLConfig.getProperty("flyway.endpoint")}',
               |  'fs.oss.accessKeyId' = '${ETLConfig.getProperty("flyway.ak")}',
               |  'fs.oss.accessKeySecret' = '${ETLConfig.getProperty("flyway.sk")}'
               |)""".stripMargin)
          ETLFlinkSession.batchEnv.useCatalog(catalogName)
          session.executeSql(s"CREATE DATABASE IF NOT EXISTS ${ETLConfig.getProperty("flyway.database")}")
        } else {
          throw new RuntimeException(s"catalog $catalogName not found")
        }
      }
    }
  }
}
