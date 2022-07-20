-- workflow=order_report_latest
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=postgres
--  dbName=postgres
--  tableName=dwd.t_fact_order
-- target=postgres
--  dbName=postgres
--  tableName=report.t_fact_order_report_latest
-- writeMode=overwrite
select
    fact.order_sn order_sn,
    dim2.product_id product_id,
    dim2.mid product_code,
    dim2.name product_name,
    dim2.version product_version,
    dim2.status product_status,
    fact.price price,
    fact.discount discount,
    fact.order_status order_status,
    fact.order_create_time order_create_time,
    fact.order_update_time order_update_time,
    fact.actual actual
from dwd.t_fact_order fact
         inner join dwd.t_dim_product dim on fact.product_id = dim.product_id
         inner join (select * from dwd.t_dim_product dim_latest where is_latest='1') dim2 on dim.mid = dim2.mid;