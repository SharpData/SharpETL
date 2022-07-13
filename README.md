## Sharp ETL

[![Build Status](https://dev.azure.com/izhangzhihao/Sharp%20ETL/_apis/build/status/SharpData.SharpETL?branchName=main)](https://dev.azure.com/izhangzhihao/Sharp%20ETL/_build/latest?definitionId=5&branchName=main) [![License](https://img.shields.io/badge/license-Apache%202-brightgreen.svg)](https://github.com/SharpData/SharpETL/blob/main/LICENSE) [![codecov](https://codecov.io/gh/SharpData/SharpETL/branch/main/graph/badge.svg?token=299D3CIJ7Y)](https://codecov.io/gh/SharpData/SharpETL)

Sharp ETL is a ETL framework that simplifies writing and executing ETLs by simply writing SQL workflow files.
The SQL workflow file format is combined your favourite SQL dialects with just a little bit of configurations.

## Getting started

### Let's run a sharp etl mysql db first

```shell
docker run --name sharp_etl_db -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=sharp_etl mysql:5.7
```

### build or download jar

```shell
./gradlew buildJars -PscalaVersion=2.12 -PsparkVersion=3.3.0 -PscalaCompt=2.12.15
```

### save the content as file `hello_world.sql`

```sql
-- workflow=hello_world
--  loadType=incremental
--  logDrivenType=timewindow

-- step=print SUCCESS to console
-- source=temp
-- target=console

SELECT 'SUCCESS' AS `RESULT`;
```

### run and check the console output

```shell
spark-submit --master local --class com.github.sharpdata.sharpetl.spark.Entrypoint spark/build/libs/sharp-etl-spark-1.0.0-SNAPSHOT.jar single-job --name=hello_world --period=1440 --default-start-time="2022-07-01 00:00:00" --once --local
```

And you will see the output like:

```
== Physical Plan ==
*(1) Project [SUCCESS AS RESULT#17167]
+- Scan OneRowRelation[]
root
 |-- RESULT: string (nullable = false)

+-------+
|RESULT |
+-------+
|SUCCESS|
+-------+
```


## Versions and dependencies

The compatible versions of [Spark](http://spark.apache.org/) are as follows:

| Spark | Scala
| ----- | --------
| 2.3.x | 2.11
| 2.4.x | 2.11 / 2.12
| 3.0.x | 2.12 / 2.13
| 3.1.x | 2.12 / 2.13
| 3.2.x | 2.12 / 2.13
| 3.3.x | 2.12 / 2.13

