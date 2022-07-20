-- workflow=order_report_actual
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=postgres
--  dbName=postgres
--  tableName=dwd.t_fact_order
-- target=postgres
--  dbName=postgres
--  tableName=report.t_fact_order_report_actual
-- writeMode=overwrite
select
    fact.order_sn order_sn,
    dim.product_id product_id,
    dim.mid product_code,
    dim.name product_name,
    dim.version product_version,
    dim.status product_status,
    fact.price price,
    fact.discount discount,
    fact.order_status order_status,
    fact.order_create_time order_create_time,
    fact.order_update_time order_update_time,
    fact.actual actual
from dwd.t_fact_order fact
         inner join dwd.t_dim_product dim
                    on fact.product_id = dim.product_id;