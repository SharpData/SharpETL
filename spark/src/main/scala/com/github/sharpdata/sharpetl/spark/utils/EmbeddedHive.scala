package com.github.sharpdata.sharpetl.spark.utils

import com.github.sharpdata.sharpetl.spark.extension.UdfInitializer
import ETLSparkSession.conf
import ShutdownHookManager.registerShutdownDeleteDir
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.spark.sql.SparkSession

import java.io.{File, IOException}
import java.util.UUID

// scalastyle:off
object EmbeddedHive {
  lazy val tempDir: File = createTempDir()
  lazy val localMetastorePath: String = new File(tempDir, "metastore").getCanonicalPath
  lazy val localWarehousePath: String = new File(tempDir, "warehouse").getCanonicalPath

  def newBuilder(): SparkSession.Builder = {
    val builder = SparkSession.builder()
    // Long story with lz4 issues in 2.3+
    builder.config("spark.io.compression.codec", "snappy")
    // We have to mask all properties in hive-site.xml that relates to metastore
    // data source as we used a local metastore here.
    import org.apache.hadoop.hive.conf.HiveConf
    val hiveConfVars = HiveConf.ConfVars.values()
    val accessiableHiveConfVars = hiveConfVars.map(WrappedConfVar)
    accessiableHiveConfVars.foreach { confvar =>
      if (confvar.varname.contains("datanucleus") ||
        confvar.varname.contains("jdo")) {
        builder.config(confvar.varname, confvar.getDefaultExpr())
      }
    }
    builder.config(HiveConf.ConfVars.METASTOREURIS.varname, "")
    builder.config("javax.jdo.option.ConnectionURL",
      s"jdbc:derby:;databaseName=$localMetastorePath;create=true")
    builder.config("datanucleus.rdbms.datastoreAdapterClassName",
      "org.datanucleus.store.rdbms.adapter.DerbyAdapter")
    builder.config("spark.sql.warehouse.dir",
      localWarehousePath)
    builder
  }

  lazy val sparkWithEmbeddedHive: SparkSession = {
    val session =
      newBuilder()
        .master("local")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.catalogImplementation", "hive")
        .config(conf())
        .enableHiveSupport()
        .getOrCreate()
    UdfInitializer.init(session)
    session
  }


  def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File = {
    val dir = createDirectory(root)
    registerShutdownDeleteDir(dir)
    dir
  }

  def createDirectory(root: String): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException(
          s"Failed to create a temp directory (under ${root}) after ${maxAttempts}")
      }
      try {
        dir = new File(root, "spark-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch {
        case _: SecurityException => dir = null;
      }
    }

    dir
  }
}


case class WrappedConfVar(cv: ConfVars) {
  val varname = cv.varname

  def getDefaultExpr(): String = cv.getDefaultExpr()
}
// scalastyle:on
