-- workflow=daily_jobs_summary_report_test
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.DailyJobsSummaryReportTransformer
--  methodName=transform
--  transformerType=object
--  datasource=hive,postgres
-- target=do_nothing