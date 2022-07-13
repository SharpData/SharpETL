-- step=1
-- source=hive
-- target=mysql
--  dbName=cp_dmp_dm
--  tableName=ADS_SALES_SO
-- writeMode=upsert
SELECT LOCATION,
       PROD_ID,
       COMPANY_TYPE,
       SO_NUM,
       `DATE`,
       WEEK_TO_DAY,
       MONTH_TO_DAY,
       LAST_WEEK,
       SAME_DAY_LAST_YEAR_SO,
       YESTERDAY_SO,
       LAST_7_DAY,
       LAST_14_DAY,
       LAST_21_DAY,
       AVG_WEEK_DAILY,
       AVG_MONTH_DAILY,
       LAST_MONTH_AVG_MONTH_DAILY,
       DM_INSERT_DATE_HOUR
FROM DM_SALES_SO
WHERE DM_INSERT_DATE_HOUR = '2021012702'
  AND `date` >= '2020-08-01' and `date` < '2020-10-01'
limit 10;
