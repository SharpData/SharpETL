## Sharp ETL

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
