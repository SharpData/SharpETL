-- workflow=jobDependencyCheck
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow


-- step=1
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.JobDependencyCheckTransformer
--  methodName=transform
--  transformerType=object
--  dependencies=task-a,task-b
-- target=do_nothing

