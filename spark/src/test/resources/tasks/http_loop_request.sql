-- workflow=default_to_temp_source
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=temp
-- target=temp
--  tableName=temp_table
select 'test_1' as `table_name`
union all
select 'test_2' as `table_name`
union all
select 'test_3' as `table_name`
union all
select 'test_4' as `table_name`

-- step=2
-- source=http
--  url=http://localhost:1080/get_from_table/${table_name}
--  fieldName=result
-- target=temp
--  tableName=target_temp_table
-- loopOver=temp_table
