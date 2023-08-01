<h1 align="center">Sharp ETL</h1>
<div align="center">
    <a href="https://sharpdata.github.io/SharpETL">
        <img src="https://sharpdata.github.io/SharpETL/img/sharp_etl.png" width="320" height="320" alt="logo"/>
    </a>
</div>

<p align="center">
<a href="https://github.com/SharpData/SharpETL/actions/workflows/build.yml"><img src="https://github.com/SharpData/SharpETL/actions/workflows/build.yml/badge.svg?branch=main"></a>
<a href="https://github.com/SharpData/SharpETL/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202-brightgreen.svg"></a>
<a href="https://codecov.io/gh/SharpData/SharpETL"><img src="https://codecov.io/gh/SharpData/SharpETL/branch/main/graph/badge.svg?token=299D3CIJ7Y"></a>
<a><img src="https://img.shields.io/badge/Project%20Stage-Production%20Ready-brightgreen.svg"></a>
<a href="https://app.fossa.com/projects/git%2Bgithub.com%2FSharpData%2FSharpETL?ref=badge_shield"><img src="https://app.fossa.com/api/projects/git%2Bgithub.com%2FSharpData%2FSharpETL.svg?type=shield"></a>
</p> 

Sharp ETL is an ETL framework that simplifies writing and executing ETLs by simply writing SQL workflow files.
The SQL workflow file format is combined your favorite SQL dialects with just a little bit of configuration.

## Getting started

### Let's start a sharp ETL system db first

```shell
docker run --name sharp_etl_db -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=sharp_etl mysql:5.7
```

### build from source or download jar from [releases](https://github.com/SharpData/SharpETL/releases)

```shell
./gradlew buildJars -PscalaVersion=2.12 -PsparkVersion=3.3.0 -PscalaCompt=2.12.15
```

### Take a look at `hello_world.sql`

```shell
cat spark/src/main/resources/tasks/hello_world.sql
```

you will see the following contents:

```sql
-- workflow=hello_world
--  loadType=incremental
--  logDrivenType=timewindow

-- step=define variable
-- source=temp
-- target=variables

SELECT 'RESULT' AS `OUTPUT_COL`;

-- step=print SUCCESS to console
-- source=temp
-- target=console

SELECT 'SUCCESS' AS `${OUTPUT_COL}`;
```

### Run and check the console output

```shell
spark-submit --master local --class com.github.sharpdata.sharpetl.spark.Entrypoint spark/build/libs/sharp-etl-spark-standalone-3.3.0_2.12-0.1.0.jar single-job --name=hello_world --period=1440 --default-start-time="2022-07-01 00:00:00" --once --local
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
| 3.0.x | 2.12
| 3.1.x | 2.12
| 3.2.x | 2.12 / 2.13
| 3.3.x | 2.12 / 2.13
| 3.4.x | 2.12 / 2.13



## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FSharpData%2FSharpETL.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2FSharpData%2FSharpETL?ref=badge_large)
