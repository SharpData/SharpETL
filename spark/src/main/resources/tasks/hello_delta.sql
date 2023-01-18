-- workflow=hello_delta
--  loadType=incremental
--  logDrivenType=timewindow

-- step=create database
-- target=delta_lake
create database delta_db location 'oss://sh-qa-emr-ack/data/delta_db';


-- step=create table
-- target=delta_lake
create or replace table delta_db.delta_tbl
(
    id   INT,
    name STRING
) using delta options(path='oss://sh-qa-emr-ack/data/delta_db/delta_tbl');


-- step=insert some sample data
-- target=delta_lake
insert into delta_db.delta_tbl
values (1, "a1"),
       (2, "a2");

-- step=print data to console
-- source=delta_lake
-- target=console
select * from delta_db.delta_tbl;

-- step=update sample data
-- target=delta_lake
update delta_db.delta_tbl name = 'a1_new' where id = 1;

-- step=delete sample data
-- target=delta_lake
delete from delta_db.delta_tbl where id = 2;

-- step=print updated data to console
-- source=delta_lake
-- target=console
select * from delta_db.delta_tbl;