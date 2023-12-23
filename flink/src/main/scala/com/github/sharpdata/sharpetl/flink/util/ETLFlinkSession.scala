package com.github.sharpdata.sharpetl.flink.util

import com.github.sharpdata.sharpetl.core.quality.QualityCheckRule
import com.github.sharpdata.sharpetl.core.repository.QualityCheckAccessor
import com.github.sharpdata.sharpetl.core.util.Constants.ETLDatabaseType.FLINK_SHARP_ETL
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, ETLLogger}
import com.github.sharpdata.sharpetl.flink.job.FlinkWorkflowInterpreter
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.TableEnvironment

object ETLFlinkSession {
  var local = false
  var wfName = "default"
  //  private var sparkConf: SparkConf = _
  private var autoCloseSession: Boolean = true

  private val env: StreamExecutionEnvironment = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment(getConf())
    environment.setRuntimeMode(RuntimeExecutionMode.BATCH)
    environment
  }

  lazy val sparkSession: TableEnvironment = {
    //TableEnvironment.create(env)
    batchEnv
  }

  val batchSettings: EnvironmentSettings = EnvironmentSettings.newInstance
    .withConfiguration(getConf())
    //.inStreamingMode
    .inBatchMode
    .build()

  val batchEnv: TableEnvironment = TableEnvironment.create(batchSettings)

  //  def conf(): SparkConf = {
  //    if (sparkConf == null) {
  //      ETLLogger.info("init tEnv conf...")
  //      sparkConf = new SparkConf()
  //      if (!sparkConf.contains("tEnv.master")) {
  //        sparkConf.setMaster("local[*]")
  //      }
  //      sparkConf.set("tEnv.sql.legacy.timeParserPolicy", "LEGACY")
  //      setSparkConf(sparkConf)
  //    }
  //    sparkConf
  //  }

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

  //  @inline def getHiveSparkSession(): SparkSession = sparkSession

  def getFlinkInterpreter(local: Boolean,
                          wfName: String,
                          autoCloseSession: Boolean,
                          etlDatabaseType: String,
                          dataQualityCheckRules: Map[String, QualityCheckRule])
  : FlinkWorkflowInterpreter = {
    ETLFlinkSession.local = local
    ETLFlinkSession.wfName = wfName
    ETLFlinkSession.autoCloseSession = autoCloseSession
    val session = ETLFlinkSession.sparkSession
    //UdfInitializer.init(session)
    createCatalogIfNeed(etlDatabaseType, session)
    new FlinkWorkflowInterpreter(session, dataQualityCheckRules, QualityCheckAccessor.getInstance(etlDatabaseType))
  }

  //  def release(spark: SparkSession): Unit = {
  //    if (spark != null && autoCloseSession) {
  //      spark.stop()
  //    }
  //  }

  def createCatalogIfNeed(etlDatabaseType: String, session: TableEnvironment) = {
    if (etlDatabaseType == FLINK_SHARP_ETL) {
      val catalogName = ETLConfig.getProperty("flyway.catalog")
      val catalog = session.getCatalog(catalogName)
      if (!catalog.isPresent) {
        if (local) {
          ETLLogger.info(s"catalog $catalogName not found, create it")
          session.executeSql(s"CREATE CATALOG $catalogName WITH ('type' = 'paimon', 'warehouse' = '${ETLConfig.getProperty("flyway.warehouse")}')")
          ETLFlinkSession.batchEnv.useCatalog(catalogName)
          session.executeSql(s"CREATE DATABASE IF NOT EXISTS ${ETLConfig.getProperty("flyway.database")}")
        } else {
          throw new RuntimeException(s"catalog $catalogName not found")
        }
      }
    }
  }
}
