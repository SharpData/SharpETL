-- workflow=mysql_to_paimon
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=define source catalog
-- source=ddl
-- target=do_nothing
CREATE CATALOG sharp_etl_db
WITH ( 'type' = 'jdbc',
    'default-database' = 'sharp_etl',
    'username' = 'root',
    'password' = 'root',
    'base-url' = 'jdbc:mysql://localhost:3306'
);

-- step=define paimon catalog
-- source=ddl
-- target=do_nothing

CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///Users/izhangzhihao/Downloads/sharp-etl/paimon-warehouse'
);

-- step=define paimon sink table
-- source=ddl
-- target=do_nothing

CREATE TABLE IF NOT EXISTS `paimon`.`default`.flyway_schema_history (
    `id` INT,
    `version` VARCHAR,
    `description` VARCHAR,
    `type` VARCHAR,
    `script` VARCHAR,
    `checksum` BIGINT,
    `installed_by` VARCHAR,
    `installed_on` TIMESTAMP,
    `execution_time` INT,
    `success` BOOLEAN,
     user_action_time AS PROCTIME()
) WITH (
    'connector' = 'paimon',
    'tag.automatic-creation' = 'process-time',
    'tag.creation-period' = 'daily'
);

-- step=insert into paimon sink table
-- source=do_nothing
-- target=built_in
--  tableName=`paimon`.`default`.flyway_schema_history

SELECT * FROM `sharp_etl_db`.`sharp_etl`.`flyway_schema_history`;