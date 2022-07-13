-- workflow=from_step_id
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=transformation
--  className=some.thing.does.not.exists
--  methodName=transform
--  transformerType=dynamic_object
-- target=temp
--  tableName=`dynamic_tmp_transformer_result_table`


-- step=2
-- source=temp
-- target=temp
--  tableName=do_nothing_table
SELECT 'success';
