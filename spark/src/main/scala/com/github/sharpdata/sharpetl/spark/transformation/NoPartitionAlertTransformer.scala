package com.github.sharpdata.sharpetl.spark.transformation

import com.github.sharpdata.sharpetl.datasource.hive.HiveMetaStoreUtil
import com.github.sharpdata.sharpetl.core.exception.Exception.PartitionNotFoundException
import com.github.sharpdata.sharpetl.core.util.ETLLogger
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession
import org.apache.spark.sql
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.ql.metadata.Hive

// $COVERAGE-OFF$
object NoPartitionAlertTransformer extends Transformer {
  override def transform(args: Map[String, String]): sql.DataFrame = {

    val databaseName = args("databaseName")
    val tables = args("tables").split(",").map(_.trim).toList
    val interval = args("interval").toInt
    val unit = args("unit")
    val endDate= DateTime.parse(args("endDate"), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))

    var parNotFoundTables: List[String] = List.empty

    unit.toUpperCase match {
      case "DAY" =>
        val partitionsToScan = dailyPartitionsToScan(endDate, interval)
        parNotFoundTables =  tables.filter(table => {
          !partitionExist(databaseName, table, partitionsToScan)
        })
      case _  => throw new IllegalArgumentException(s"Unsupported date interval type UNIT:[${unit}]")
    }

    if(parNotFoundTables.nonEmpty){
      val errMessage = s"Can not found partitions within interval [${interval} ${unit}] for " +
        s"tables: [${parNotFoundTables.mkString(",")}], please check the correspond job status"
      throw new PartitionNotFoundException(errMessage)
    }

    ETLSparkSession.sparkSession.emptyDataFrame
  }

  def partitionExist(databaseName:String, tableName: String, partitions:List[String]): Boolean = {

    partitions.exists(partition => {
      var partitionNum: Integer = 0
      try {
        Hive.get().getMSC().getNumPartitionsByFilter(databaseName, tableName, partition)
      } catch {
        case e: Exception =>
          ETLLogger.warn(s"Exception occur when get partition[${partition.mkString(",")}] " +
            s"from table[${databaseName}.${tableName}] with exception:[${e.getMessage}]")
      }
      partitionNum > 0
    })

  }


  def dailyPartitionsToScan(end: DateTime, interval: Integer): List[String] ={
     (0 until interval + 1)
      .map(end.minusDays)
      .map(buildDailyPartitionPartVal)
      .toList
  }

  def buildDailyPartitionPartVal(date: DateTime): String = {
    List(s"""year="${date.getYear}"""",
      s"""month="${DateTimeFormat.forPattern("MM").print(date)}"""",
      s"""day="${DateTimeFormat.forPattern("dd").print(date)}""""
    ).mkString(" AND ")
  }
}
// $COVERAGE-ON$
