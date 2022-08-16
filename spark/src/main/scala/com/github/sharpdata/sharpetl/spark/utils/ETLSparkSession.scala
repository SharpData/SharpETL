package com.github.sharpdata.sharpetl.spark.utils

import com.github.sharpdata.sharpetl.core.quality.QualityCheckRule
import com.github.sharpdata.sharpetl.core.repository.QualityCheckAccessor
import com.github.sharpdata.sharpetl.core.util.Constants.Environment
import com.github.sharpdata.sharpetl.core.util.ETLConfig.purgeHiveTable
import com.github.sharpdata.sharpetl.core.util.{ETLConfig, ETLLogger}
import com.github.sharpdata.sharpetl.spark.extension.UdfInitializer
import com.github.sharpdata.sharpetl.spark.job.SparkWorkflowInterpreter
import com.github.sharpdata.sharpetl.spark.utils.EmbeddedHive.sparkWithEmbeddedHive
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ETLSparkSession {
  var local = false
  private var wfName = "default"
  private var sparkConf: SparkConf = _
  private var autoCloseSession: Boolean = true

  lazy val sparkSession: SparkSession = {
    if (Environment.CURRENT == Environment.EMBEDDED_HIVE) {
      sparkWithEmbeddedHive
    } else if (local) {
      SparkSession.builder().config(conf()).getOrCreate()
    } else {
      //System.setProperty("atlas.conf", "/usr/hdp/current/spark2-client/conf/")
      SparkSession.builder().config(conf()).enableHiveSupport().getOrCreate()
    }
  }

  def conf(): SparkConf = {
    if (sparkConf == null) {
      ETLLogger.info("init spark conf...")
      sparkConf = new SparkConf()
      if (!sparkConf.contains("spark.master")) {
        sparkConf.setMaster("local[*]")
      }
      setSparkConf(sparkConf)
      sparkConf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    }
    sparkConf
  }

  private def setSparkConf(sparkConf: SparkConf): Unit = {
    ETLConfig
      .getSparkProperties(wfName)
      .foreach {
        case (key, value) =>
          sparkConf.set(key, value)
          ETLLogger.info(s"[Set spark config]: $key=$value")
      }
  }

  @inline def getHiveSparkSession(): SparkSession = sparkSession

  def getSparkInterpreter(local: Boolean,
                          wfName: String,
                          autoCloseSession: Boolean,
                          etlDatabaseType: String,
                          dataQualityCheckRules: Map[String, QualityCheckRule])
  : SparkWorkflowInterpreter = {
    ETLSparkSession.local = local
    ETLSparkSession.wfName = wfName
    ETLSparkSession.autoCloseSession = autoCloseSession
    val spark = ETLSparkSession.sparkSession
    UdfInitializer.init(spark)
    new SparkWorkflowInterpreter(spark, dataQualityCheckRules, QualityCheckAccessor.getInstance(etlDatabaseType))
  }

  def release(spark: SparkSession): Unit = {
    if (spark != null && autoCloseSession) {
      spark.stop()
    }
  }

  def autoPurgeHiveTable(table: String): Unit = {
    if (sparkSession.catalog.tableExists(table)) {
      purgeHiveTable match {
        case "true" => sparkSession.sql(s"ALTER TABLE $table SET TBLPROPERTIES('auto.purge' = 'true')")
        case "false" => sparkSession.sql(s"ALTER TABLE $table SET TBLPROPERTIES('auto.purge' = 'false')")
        case "none" => () //follow the original tblproperties of table.
      }
    }
  }
}
