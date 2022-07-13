-- step=1
-- source=informix
--  dbName=sysmaster
--  tableName=online_order
-- target=postgres
--  dbName=postgres
--  tableName=ods.t_fact_online_order
-- writeMode=append
SELECT order_no AS order_no,
       user_id AS user_id,
       user_name AS user_name,
       order_total_amount AS order_total_amount,
       actual_amount AS actual_amount,
       post_amount AS post_amount,
       order_pay_amount AS order_pay_amount,
       total_discount AS total_discount,
       pay_type AS pay_type,
       source_type AS source_type,
       order_status AS order_status,
       note AS note,
       confirm_status AS confirm_status,
       payment_time AS payment_time,
       delivery_time AS delivery_time,
       receive_time AS receive_time,
       comment_time AS comment_time,
       delivery_company AS delivery_company,
       delivery_code AS delivery_code,
       business_date AS business_date,
       return_flag AS return_flag,
       created_at AS created_at,
       updated_at AS updated_at,
       deleted_at AS deleted_at,
       ${JOB_ID} AS job_id,
       CURRENT YEAR TO SECOND AS job_time
FROM online_order
WHERE business_date >= TO_DATE('${DATA_RANGE_START}') AND business_date < TO_DATE('${DATA_RANGE_END}');