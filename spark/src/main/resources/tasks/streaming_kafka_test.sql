-- workflow=streaming_kafka_test
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=temp
-- target=temp
--  tableName=public_temp
with data as (select 'user_id_1'   as user_id,
                     'user_name_1' as user_name
              union all
              select 'user_id_2'   as user_id,
                     'user_name_2' as user_name)
select *
from data;

-- step=2
-- source=streaming_kafka
--  topics=streaming_kafka_test
--  groupId=streaming_kafka_test_group_1
--  interval=10
--  schemaDDL=`day` STRING,`dataType` STRING,`info_1` STRING,`info_2` STRING,`list` ARRAY<STRING>
-- target=temp
--  tableName=kafka_temp

-- step=3
-- source=temp
-- target=console
with public_data as (select user_id,
                            user_name
                     from public_temp),
     kafka_data as (select `day`,
                           `data_type`,
                           `info_1`,
                           `info_2`,
                           `list`
                    from kafka_temp),
     explode_data as (select `day`,
                             `data_type`,
                             `info_1`,
                             `info_2`,
                             explode(`list`) as `json`
                      from kafka_data),
     lateral_view_data as (select `day`,
                                  `data_type`,
                                  `info_1`,
                                  `info_2`,
                                  j.*
                           from explode_data
                                    lateral view
                                        json_tuple(json, 'id', 'time', 'user_id') j as `id`, `time`, `user_id`)
select *
from lateral_view_data
         left semi
         join public_data
              on lateral_view_data.user_id = public_data.user_id;
