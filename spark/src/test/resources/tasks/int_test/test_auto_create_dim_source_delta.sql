-- workflow=test_auto_create_dim_source_delta
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=create database
-- target=delta_lake
CREATE SCHEMA IF NOT EXISTS delta_db;


-- step=create table
-- target=delta_lake
create or replace table delta_db.test_fact
(
    id         STRING,
    bz_time    TIMESTAMP
) using delta;


-- step=write data
-- target=delta_lake
--  dbName=delta_db
--  tableName=test_fact
-- writeMode=overwrite
select '1' as id,
       '2022-02-02 17:12:59' as bz_time;

-- step=print data to console
-- source=delta_lake
-- target=console
select * from delta_db.test_fact;
