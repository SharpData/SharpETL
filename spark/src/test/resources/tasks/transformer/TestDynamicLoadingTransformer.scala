import com.github.sharpdata.sharpetl.spark.transformation.Transformer
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession
import org.apache.spark.sql.DataFrame
import com.github.sharpdata.sharpetl.spark.utils.ETLSparkSession.sparkSession

object TestDynamicLoadingTransformer extends Transformer {
  override def transform(args: Map[String, String]): DataFrame = {
    val spark = ETLSparkSession.sparkSession
    val schema = List(
      org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.IntegerType, true),
      org.apache.spark.sql.types.StructField("name", org.apache.spark.sql.types.StringType, true)
    )

    val data = Seq(
      org.apache.spark.sql.Row(1, args("jobId"))
    )

    val testDf = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      org.apache.spark.sql.types.StructType(schema)
    )

    testDf.createOrReplaceTempView("test_tmp_view")

    (
      spark.sql("select id, name, 'aa' as address from test_tmp_view").union(
        sparkSession.sql("select 'c' as id, 'd' as name, 'cc' as address")
      )
    ).drop("address")
  }
}
