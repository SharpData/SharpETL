-- step=1
-- source=temp
-- target=batch_kafka
--  topics=sink-to-kafka-6
with data as (select 'user_id_1'   as `user_id`,
                     'user_name_1' as `user_name`
              union all
              select 'user_id_2'   as `user_id`,
                     'user_name_2' as `user_name`)
select *
from data;
