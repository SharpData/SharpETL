import com.github.sharpdata.sharpetl.spark.transformation._
import com.github.sharpdata.sharpetl.core.datasource.config.{DBDataSourceConfig, YellowBrickDataSourceConfig}
import com.github.sharpdata.sharpetl.core.syntax.WorkflowStep
import com.github.sharpdata.sharpetl.core.util.Constants.DataSourceType
import com.github.sharpdata.sharpetl.spark.job.IO.write
import com.github.sharpdata.sharpetl.spark.transformation.{JdbcResultSetTransformer, Transformer}
import org.apache.spark.sql.DataFrame

import scala.util.matching.Regex

object RouteWriterTransformer extends Transformer {


  override def transform(df: DataFrame,
                         step: WorkflowStep,
                         args: Map[String, String]): Unit = {

    val dbType = args("dbType").toLowerCase
    val tableNamePattern = new Regex(args("tableNamePattern"))
    val matchFieldName = args("matchFieldName")
    val tableNamePrefix = args.getOrElse("tableNamePrefix", "")
    val isCleanTarget = args.getOrElse("isCleanTarget", "").toLowerCase()
    val isRemoveMatchField = args.getOrElse("isRemoveMatchField", "").toLowerCase()
    val deleteSql = args.getOrElse("deleteSql", "")
    val matchFieldValues = getMatchFieldValues(df, matchFieldName)
    val routeTablesRelationship = getRouteTablesRelationship(matchFieldValues, tableNamePattern)

    routeTablesRelationship.foreach(relationship => {
      if (isCleanTarget == true.toString && dbType.equals(DataSourceType.YELLOWBRICK)) {
        cleanYbTargetByDeleteSql(deleteSql, relationship._1, args("dbName").toLowerCase(), dbType)
      }
      prepareConfig(step, dbType, s"${tableNamePrefix}${relationship._1}", args)

      val currentDF = if (isRemoveMatchField.equals(true.toString)) {
        filterDfByRouteTablesRelationship(df, matchFieldName, relationship).drop(matchFieldName)
      } else {
        filterDfByRouteTablesRelationship(df, matchFieldName, relationship)
      }
      write(currentDF, step, args)
    })
  }

  def cleanYbTargetByDeleteSql(deleteSql: String, tableName: String, dbName: String, dbType: String): Unit = {
    val sql = deleteSql.replace("TABLE_NAME", tableName)
    val map = Map[String, String]("sql" -> sql, "dbName" -> dbName, "dbType" -> dbType)
    JdbcResultSetTransformer.transform(map)
  }

  def filterDfByRouteTablesRelationship(df: DataFrame, matchFieldName: String, relationship: (String, String)): DataFrame = {
    df.filter(df(matchFieldName) === relationship._2)
  }

  def getMatchFieldValues(df: DataFrame, matchFieldName: String): List[String] = {
    df.select(matchFieldName).distinct().collect().map(it => {
      if (it.isNullAt(0)) {
        ""
      } else {
        it.getString(0)
      }
    }).toList
  }

  def getRouteTablesRelationship(matchFieldValues: List[String], tableNamePattern: Regex): List[(String, String)] = {
    matchFieldValues.map(it => {
      tableNamePattern.findFirstMatchIn(it) match {
        case Some(x) => (x.group(2), it)
        case _ => ("", "")
      }
    }).filter(it => it._1.nonEmpty)
  }

  def prepareConfig(step: WorkflowStep,
                    tableType: String,
                    tableName: String,
                    args: Map[String, String]): Unit = {

    tableType match {
      case DataSourceType.HIVE =>
        val targetConfig = new DBDataSourceConfig()
        targetConfig.setDbName(args("dbName").toLowerCase())
        targetConfig.setTableName(tableName)
        targetConfig.dataSourceType = DataSourceType.HIVE
        step.setTargetConfig(targetConfig)

      case DataSourceType.YELLOWBRICK =>
        val targetConfig = new YellowBrickDataSourceConfig()
        targetConfig.setDbName(args("dbName").toLowerCase())
        targetConfig.setTableName(tableName)
        targetConfig.setPrimaryKeys(args.getOrElse("primaryKeys", "").toLowerCase())
        targetConfig.dataSourceType = DataSourceType.YELLOWBRICK
        step.setTargetConfig(targetConfig)
      case _ =>
        throw new RuntimeException(s"Unsupported target type: $tableType")
    }
  }
}
