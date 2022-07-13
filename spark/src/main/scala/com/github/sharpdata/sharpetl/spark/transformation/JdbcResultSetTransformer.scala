package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.spark.datasource.connection.JdbcConnection
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import java.sql.ResultSetMetaData
import scala.collection.mutable.ListBuffer

object JdbcResultSetTransformer extends Transformer {

  // scalastyle:off
  def getDataType(sourceType: String, scale: Int, precision: Int): DataType = {
    sourceType match {
      case _ if classOf[String].getName == sourceType => StringType
      case _ if classOf[Integer].getName == sourceType => IntegerType
      case _ if classOf[java.lang.Short].getName == sourceType => ShortType
      case _ if classOf[java.lang.Long].getName == sourceType => LongType
      case _ if classOf[java.math.BigDecimal].getName == sourceType => DecimalType(precision, scale)
      case _ if classOf[java.sql.Timestamp].getName == sourceType => TimestampType
    }
  }
  // scalastyle:on

  def getSparkSchema(meta: ResultSetMetaData): ListBuffer[StructField] = {
    val count = meta.getColumnCount
    val res = ListBuffer[StructField]()
    for (i <- 1 to count) {
      res += StructField(
        meta.getColumnLabel(i),
        getDataType(meta.getColumnClassName(i), meta.getScale(i), meta.getPrecision(i)))
    }
    res
  }

  def getResultSet(args: collection.Map[String, String]): DataFrame = {
    val query: String = args("sql")

    val conn = JdbcConnection(args("dbName"), args("dbType")).getConnection()
    val stat = conn.createStatement()
    stat.execute(query)
    val rs = stat.getResultSet
    if(rs != null) {
      val objs: ListBuffer[ListBuffer[Object]] = ListBuffer[ListBuffer[Object]]()
      val schema = StructType(getSparkSchema(rs.getMetaData).toSeq)
      while (rs.next()) {
        val row: ListBuffer[Object] = ListBuffer[Object]()
        val count = rs.getMetaData.getColumnCount
        for (i <- 1 to count) {
          row += rs.getObject(i)
        }
        objs += row
      }
      val rows: Seq[Row] = objs.map(v => Row.fromSeq(v.toSeq)).toSeq
      ETLSparkSession
        .sparkSession
        .createDataFrame(ETLSparkSession.sparkSession.sparkContext.parallelize(rows), schema)
    } else {
      ETLSparkSession.sparkSession.emptyDataFrame
    }
  }

  override def transform(args: Map[String, String]): DataFrame = {
    getResultSet(args)
  }
}
