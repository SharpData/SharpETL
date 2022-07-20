-- workflow=excel_test
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=excel
--  onlyOneName=true
--  configPrefix=cp_dmp
--  fileNamePattern=()(\d{4}年\d{2}月国代任务数据\.xlsx)()
--  dataAddress=A1
-- target=temp
--  tableName=temp

-- step=2
-- source=temp
-- target=variables
select from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyyMMddHH'), 'yyyyMMdd') as `DATE_END`;

-- step=3
-- source=temp
-- target=console
select `Level`           as level,
       `Spu_code`        as spu_code,
       `Sku_code`        as sku_code,
       `系列`              as series,
       `型号`              as model,
       `制式`              as standard,
       `SI任务`            as si_task,
       `SO任务`            as so_task,
       `市场健康度-A类-目标值`    as market_health_a_target_value,
       `市场健康度-A类-底线值`    as market_health_a_bottom_value,
       `市场健康度-B类-目标值`    as market_health_b_target_value,
       `市场健康度-B类-底线值`    as market_health_b_bottom_value,
       `市场健康度-二级市场管控-目标` as market_health_2_target_value,
       `file_name`       as file_name,
       ${TASK_ID}        as task_id,
       ${DATE_END}       as ods_insert_date
from temp;
