-- workflow=route_writer_transformer
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow


-- step=1
-- source=temp
-- target=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.RouteWriterTransformer
--  methodName=transform
--  transformerType=dynamic_object
--  dbType=hive
--  dbName=db
--  tableNamePrefix=ods__xxx_
--  tableNamePattern=(\w*(xx_\d*).incr.txt)
--  matchFieldName=fileName
--  primaryKeys=aaa
-- writeMode=append
select * from temp;


