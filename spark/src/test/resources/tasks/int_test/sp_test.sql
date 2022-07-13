-- workflow=sp_test
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=transformation
--  dbType=mysql
--  dbName=int_test
--  className=com.github.sharpdata.sharpetl.spark.transformation.JdbcResultSetTransformer
--  methodName=transform
--  transformerType=object
-- target=mysql
--  dbName=int_test
--  tableName=sp_test
-- writeMode=append
call my_test()