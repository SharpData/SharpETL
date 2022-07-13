-- workflow=udf_test
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

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