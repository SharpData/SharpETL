---
title: "UDF guide"
sidebar_position: 10
toc: true
last_modified_at: 2021-12-23T18:25:57-04:00
---

## UDF

用户可以通过build自己的jar包来实现UDF的支持，这个jar包不需要基于Sharp ETL来实现，甚至仅仅是普通的Scala function即可。

例如，用户需要注册一个新的UDF来实现自定义逻辑，只需要编写普通代码即可：

```scala
class TestUdfObj extends Serializable {
  def testUdf(value: String): String = {
    s"$value-proceed-by-udf"
  }
}
```

打包完成后需要将jar包与Sharp ETL的jar包一起提交，这样就可以很轻易的引用自己的UDF了。

```bash
spark-submit --class com.github.sharpdata.sharpetl.spark.Entrypoint spark/build/libs/spark-1.0.0-SNAPSHOT.jar /path/to/your-udf.jar ... ...
```

```sql
-- step=1
-- source=class
--  className=com.github.sharpdata.sharpetl.spark.end2end.TestUdfObj
-- target=udf
--  methodName=testUdf
--  udfName=test_udf

-- step=2
-- source=temp
-- target=temp
--  tableName=udf_result
select test_udf('input') as `result`;
```
