-- workflow=loop_http_transformation_test
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow


-- step=1
-- source=temp
-- target=variables
select 'test1,test2,test3' as `centerIds`

-- step=2
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.LoopHttpTransformer
--  methodName=transform
--  transformerType=dynamic_object
--  url=http://localhost:1080/report?centerId=${centerId}
--  items=${centerIds}
--  splitBy=,
--  varName=centerId
-- target=temp
--  tableName=`target_db`
