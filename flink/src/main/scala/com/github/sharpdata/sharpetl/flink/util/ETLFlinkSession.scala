package com.github.sharpdata.sharpetl.flink.util

import com.github.sharpdata.sharpetl.core.quality.QualityCheckRule
import com.github.sharpdata.sharpetl.core.repository.QualityCheckAccessor
import com.github.sharpdata.sharpetl.core.util.Constants.Environment
import com.github.sharpdata.sharpetl.core.util.ETLConfig.purgeHiveTable
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, ETLLogger}
import com.github.sharpdata.sharpetl.flink.job.FlinkWorkflowInterpreter
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

object ETLFlinkSession {
  var local = false
  private var wfName = "default"
//  private var sparkConf: SparkConf = _
  private var autoCloseSession: Boolean = true

  lazy val sparkSession: StreamTableEnvironment = {
    StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment())
  }

  lazy val batchSession: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()

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

//  private def setSparkConf(sparkConf: SparkConf): Unit = {
//    ETLConfig
//      .getSparkProperties(wfName)
//      .foreach {
//        case (key, value) =>
//          sparkConf.set(key, value)
//          ETLLogger.info(s"[Set tEnv config]: $key=$value")
//      }
//  }

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
    val spark = ETLFlinkSession.sparkSession
    //UdfInitializer.init(spark)
    new FlinkWorkflowInterpreter(spark, dataQualityCheckRules, QualityCheckAccessor.getInstance(etlDatabaseType))
  }

//  def release(spark: SparkSession): Unit = {
//    if (spark != null && autoCloseSession) {
//      spark.stop()
//    }
//  }
}
