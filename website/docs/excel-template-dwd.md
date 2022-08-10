---
title: "Excel template for ods to dwd"
sidebar_position: 4
toc: true
last_modified_at: 2021-10-21T10:59:57-04:00
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## pre-requirements

* all table(fact & dim) must exist before running ETL jobs(ETL doesn't create table)
* you can download [this excel](https://docs.google.com/spreadsheets/d/1Prw1LFfkSkaAuf1K6O0TTI5PPRP7lLtzIR63x9HCSVw/edit#gid=1642393109) to your `~/Desktop` for the quick start guide.

## Case 1

Just copy all data from ods to dwd, not joining with other tables and no quality check.

<Tabs
defaultValue="markdown"
values={[
{ label: 'Excel', value: 'markdown', },
{ label: 'SQL', value: 'sql', }
]}>

<TabItem value="markdown">

| source_db_name | source_table_name |  column_name | incremental_type   | target_db_name | target_table_name | Target column_name | sort_column | id_column | quality_check_rules | dim_key | dim_sort_column | dim_description | auto_create_dim | auto_create_dim_id | dim_db_name | dim_table_name | dim_column_name | zip_dim_key | partition_column |
| :------------- | :---------------- | :----------------- | :----------------- | :------------- | :---------------- | :----------------- | :---------- | :-------- | :------------------ | :------ | :-------------- | :-------------- | :-------------- | :----------------- | :---------- | :------------- | :-------------- | :-------------- | :-------------- |
| usecase_ods    | test_cust         | id                 | incremental_append | usecase_dwd    | t_fact_test_cust  | id                 |             | TRUE      |                     |         |                 |                 |                 |                    |             |                |                 |                 |                 |
| usecase_ods    | test_cust         | code               | incremental_append | usecase_dwd    | t_fact_test_cust  | code               |             |           |                     |         |                 |                 |                 |                    |             |                |                 |                 |                 |
| usecase_ods    | test_cust         | bz_time            | incremental_append | usecase_dwd    | t_fact_test_cust  | bz_time            | TRUE        |           |                     |         |                 |                 |                 |                    |             |                |                 |                 |                 |
</TabItem>

<TabItem value="sql">

:::note
You need to rename the sheet name of Case1 to `Fact` to ensure the above command to execute successfully.
:::

When generating dimension-split table, the corresponding command line is as follows:

```bash
./gradlew clean :spark:run --args="generate-fact-sql -f ~/Desktop/数据字典-模版.xlsx --output ~/Desktop/"
```

The sql file corresponding to Case1 then will be generated on your desktop named `dwd_t_fact_test_cust.sql`:

```sql
-- step=1
-- source=temp
-- target=variables
-- checkPoint=false
-- dateRangeInterval=0
select from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMdd')               as `DATE_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'HH')                     as `HOUR_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')  as `EFFECTIVE_START_TIME`;

-- step=2
-- source=hive
--  dbName=usecase_ods
--  tableName=test_cust
--  options
--   idColumn=id
-- target=temp
--  tableName=`1d764347`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `id` as `id`,
       `code` as `code`,
       `bz_time` as `bz_time`,
       '${EFFECTIVE_START_TIME}' as effective_start_time,
       '9999-01-01 00:00:00'     as effective_end_time,
       '1'                       as is_active,
       '1'                       as is_latest,
       '${DATA_RANGE_START}'     as idempotent_key,
       '${DATE_END}'             as dw_insert_date
from `usecase_ods`.`test_cust`;

-- step=3
-- source=hive
--  dbName=usecase_dwd
--  tableName=t_fact_test_cust
-- target=temp
--  tableName=`0e5a2f6c`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `(dw_insert_date)?+.+`,
       '${DATE_END}' as `dw_insert_date`
from `usecase_dwd`.`t_fact_test_cust` `t_fact_test_cust`
where `dw_insert_date` = (select max(`dw_insert_date`)
                          from `usecase_dwd`.`t_fact_test_cust` `t_fact_test_cust`);

-- step=4
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.ZipTableTransformer
--  methodName=transform
--   dwDataLoadType=incremental
--   sortFields=bz_time
--   odsViewName=`1d764347`
--   dwViewName=`0e5a2f6c`
--   primaryFields=id
--  transformerType=object
-- target=hive
--  dbName=usecase_dwd
--  tableName=t_fact_test_cust
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=overwrite
```

</TabItem>
</Tabs>

## Case 2
The Case2 is generated by the new Excel: the dim table is firstly imported, then is the fact table.
The fact table joining with the dim table on the basis that the dim table is already existing and completed.

<Tabs
defaultValue="markdown"
values={[
{ label: 'Excel', value: 'markdown', },
{ label: 'SQL', value: 'sql', }
]}>

<TabItem value="markdown">

| source_db_name | source_table_name |  column_name | incremental_type   | target_db_name | target_table_name | Target column_name | sort_column | id_column | quality_check_rules | dim_key | dim_sort_column | dim_description | auto_create_dim | auto_create_dim_id | dim_db_name | dim_table_name | dim_column_name | zip_dim_key | partition_column |
| :------------- | :---------------- | :----------------- | :----------------- | :------------- | :---------------- | :----------------- | :---------- | :-------- | :------------------ | :------ | :-------------- | :-------------- | :-------------- | :----------------- | :---------- | :------------- | :-------------- | :------------- | :-------------- |
| usecase_ods            | test_split        | id                 | incremental_append | usecase_dwd            | test_fact_split  | id                 |             | TRUE      |                     |         |                 |                 |                 |                    |             |                |                 |                |                 
| usecase_ods            | test_split        | user_id               | incremental_append | usecase_dwd            | test_fact_split  | user_id               |             |           |                     |  TRUE       |                 |   TRUE              |                 |                    |    usecase_dwd         |     t_dim_user           |      id           |                |                 
| usecase_ods            | test_split        | user_name            | incremental_append | usecase_dwd            | test_fact_split  | user_name            |         |           |                     |         |                 |    TRUE             |                 |                    |  usecase_dwd           |    t_dim_user            |    user_name             |                |                 
| usecase_ods            | test_split        | user_account            | incremental_append | usecase_dwd            | test_fact_split  | user_account            |         |           |                     |         |                 |                 |                 |                    |     usecase_dwd        |    t_dim_user            |      user_account           |                |                 
| usecase_ods            | test_split        | bz_time            | incremental_append | usecase_dwd            | test_fact_split  | bz_time            | TRUE        |           |                     |         |                 |                 |                 |                    |             |                |                 |                |                 
| usecase_ods            | test_user         | id               | incremental_append | usecase_dwd            | t_dim_user  | id               |             |    TRUE       |                     |         |                 |                 |                 |                    |             |                |                 |                |                 
| usecase_ods            | test_user         | user_name            | incremental_append | usecase_dwd            | t_dim_user  | user_name            |         |           |                     |         |                 |                |                 |                    |             |                |                 |                |                 
| usecase_ods            | test_user         | user_account            | incremental_append | usecase_dwd            | t_dim_user  | user_account            |         |           |                     |         |                 |                 |                 |                    |             |                |                 |                |                 
| usecase_ods            | test_user         | bz_time            | incremental_append | usecase_dwd            | t_dim_user  | bz_time            | TRUE        |           |                     |         |                 |                 |                 |                    |             |                |                 |                |                 
</TabItem>


<TabItem value="sql">

:::note
You need to rename the sheet name of Case2 to `Fact` to ensure the above command to execute successfully.
Then there will be 2 sql files corresponding to Case2  generated on your desktop, the first one is named `dwd_t_dim_user.sql`:
:::

```sql
-- step=1
-- source=temp
-- target=variables
-- checkPoint=false
-- dateRangeInterval=0
select from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMdd')               as `DATE_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'HH')                     as `HOUR_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')  as `EFFECTIVE_START_TIME`;

-- step=2
-- source=hive
--  dbName=usecase_ods
--  tableName=test_user
--  options
--   idColumn=id
-- target=temp
--  tableName=`efa9e5ec`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `id` as `id`,
       `user_name` as `user_name`,
       `user_account` as `user_account`,
       `bz_time` as `bz_time`,
       '${EFFECTIVE_START_TIME}' as effective_start_time,
       '9999-01-01 00:00:00'     as effective_end_time,
       '1'                       as is_active,
       '1'                       as is_latest,
       '${DATA_RANGE_START}'     as idempotent_key,
       '${DATE_END}'             as dw_insert_date
from `usecase_ods`.`test_user`;

-- step=3
-- source=hive
--  dbName=usecase_dwd
--  tableName=t_dim_user
-- target=temp
--  tableName=`8c325c5e`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `(dw_insert_date)?+.+`,
       '${DATE_END}' as `dw_insert_date`
from `usecase_dwd`.`t_dim_user` `t_dim_user`
where `dw_insert_date` = (select max(`dw_insert_date`)
                          from `usecase_dwd`.`t_dim_user` `t_dim_user`);

-- step=4
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.ZipTableTransformer
--  methodName=transform
--   dwDataLoadType=incremental
--   sortFields=bz_time
--   odsViewName=`efa9e5ec`
--   dwViewName=`8c325c5e`
--   primaryFields=id
--  transformerType=object
-- target=hive
--  dbName=usecase_dwd
--  tableName=t_dim_user
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=overwrite
```

The second one is named `dwd_test_fact_split.sql`:

```sql
-- step=1
-- source=temp
-- target=variables
-- checkPoint=false
-- dateRangeInterval=0
select from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMdd')               as `DATE_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'HH')                     as `HOUR_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')  as `EFFECTIVE_START_TIME`;

-- step=2
-- source=hive
--  dbName=usecase_ods
--  tableName=test_split
--  options
--   idColumn=id
-- target=temp
--  tableName=`9e69b8df`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `test_split`.`id` as `id`,
       `test_split`.`bz_time` as `bz_time`,
       ifnull(`t_dim_user`.`id`, '-1') as `user_id`,
       '${EFFECTIVE_START_TIME}' as effective_start_time,
       '9999-01-01 00:00:00'     as effective_end_time,
       '1'                       as is_active,
       '1'                       as is_latest,
       '${DATA_RANGE_START}'     as idempotent_key,
       '${DATE_END}'             as dw_insert_date
from `usecase_ods`.`test_split` `test_split`
         left join `usecase_dwd`.`t_dim_user` `t_dim_user`
                   on `test_split`.`user_id` = `t_dim_user`.`id` and `t_dim_user`.is_latest = '1';

-- step=3
-- source=hive
--  dbName=usecase_dwd
--  tableName=test_fact_split
-- target=temp
--  tableName=`8efabcf7`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `(dw_insert_date)?+.+`,
       '${DATE_END}' as `dw_insert_date`
from `usecase_dwd`.`test_fact_split` `test_fact_split`
where `dw_insert_date` = (select max(`dw_insert_date`)
                          from `usecase_dwd`.`test_fact_split` `test_fact_split`);

-- step=4
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.ZipTableTransformer
--  methodName=transform
--   dwDataLoadType=incremental
--   sortFields=bz_time
--   odsViewName=`9e69b8df`
--   dwViewName=`8efabcf7`
--   primaryFields=id
--  transformerType=object
-- target=hive
--  dbName=usecase_dwd
--  tableName=test_fact_split
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=overwrite
```

</TabItem>

</Tabs>

## Case 3

Case 3 is generated by joining with the dim table which has the single primary key. 

<Tabs
defaultValue="markdown"
values={[
{ label: 'Excel', value: 'markdown', },
{ label: 'SQL', value: 'sql', }
]}>

<TabItem value="markdown">

| source_db_name | source_table_name |  column_name | incremental_type   | target_db_name | target_table_name | Target column_name | sort_column | id_column | quality_check_rules | dim_key | dim_sort_column | dim_description | auto_create_dim | auto_create_dim_id | dim_db_name | dim_table_name | dim_column_name | zip_dim_key | partition_column |
| :------------- | :---------------- | :----------------- | :----------------- | :------------- | :---------------- | :----------------- | :---------- | :-------- | :------------------ | :------ | :-------------- | :-------------- | :-------------- | :----------------- | :---------- | :------------- | :-------------- | :------------- | :-------------- |
| usecase_ods            | test_fact_case_3         | id                 | incremental_append | usecase_dwd            | test_fact_target_case_3  | id                 |             | TRUE      |                     |         |                 |                 |                 |                    |             |                |                 |                |                 |
| usecase_ods            | test_fact_case_3         | real_cust_id               | incremental_append | usecase_dwd            | test_fact_target_case_3  | real_cust_id  |             |           |   null check, power null check                  |  TRUE       |                 |                 |     TRUE            |                    |    usecase_dwd         |     test_cust_case_3           |      id           |                |                 |
| usecase_ods            | test_fact_case_3         | real_cust_code            | incremental_append | usecase_dwd            | test_fact_target_case_3  | real_cust_code            |         |           |                     |         |                 |       TRUE          |     TRUE            |                    |  usecase_dwd           |    test_cust_case_3            |    code             |                |                 |
| usecase_ods            | test_fact_case_3         | real_cust_bz_time            | incremental_append | usecase_dwd            | test_fact_target_case_3  | real_cust_bz_time            |         |           |                     |         |      TRUE           |    TRUE             |      TRUE           |                    |     usecase_dwd        |    test_cust_case_3            |      bz_time           |                |                 |
| usecase_ods            | test_fact_case_3         | bz_time            | incremental_append | usecase_dwd            | test_fact_target_case_3  | bz_time            | TRUE        |           |                     |         |                 |                 |                 |                    |             |                |                 |                |                 |
</TabItem>

<TabItem value="sql">

After running the same command, the sql file named `default_test_fact_target.sql` corresponding to Case3  will be generated:

```sql
-- step=1
-- source=temp
-- target=variables
-- checkPoint=false
-- dateRangeInterval=0
select from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMdd')               as `DATE_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'HH')                     as `HOUR_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')  as `EFFECTIVE_START_TIME`;

-- step=2
-- source=hive
--  dbName=usecase_ods
--  tableName=test_fact_case_3
--  options
--   idColumn=id
--   column.real_cust_id.qualityCheckRules=null check, power null check
-- target=temp
--  tableName=`a8cc8c22`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select *
from `usecase_ods`.`test_fact_case_3`;

-- step=3
-- source=temp
-- target=temp
--  tableName=`ce47db66`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
with join_fact_temp as (select nullif(`test_fact_case_3`.`real_cust_id`, `test_cust_case_3`.`id`) as `real_cust_id`,
                               nullif(`test_fact_case_3`.`real_cust_code`, `test_cust_case_3`.`code`) as `real_cust_code`,
                               nullif(`test_fact_case_3`.`real_cust_bz_time`, `test_cust_case_3`.`bz_time`) as `real_cust_bz_time`
                        from `a8cc8c22` `test_fact_case_3`
                                 left join `usecase_dwd`.`test_cust_case_3` `test_cust_case_3`
                                           on `test_fact_case_3`.`real_cust_id` = `test_cust_case_3`.`id`
                                               and `test_cust_case_3`.`is_latest` = '1'),
     distinct_dim_temp as (select `real_cust_id`,
                                  `real_cust_code`,
                                  `real_cust_bz_time`
                           from join_fact_temp
                           where 1 = 1
                             and (`real_cust_id` is not null)
                           group by `real_cust_id`, `real_cust_code`, `real_cust_bz_time`
    grouping sets (
    ( `real_cust_id`, `real_cust_code`, `real_cust_bz_time`)
    ))
select `real_cust_id` as `real_cust_id`,
       first_value(`real_cust_code`) as `real_cust_code`,
       first_value(`real_cust_bz_time`) as `real_cust_bz_time`,
       count(1) as `distinct_count_num`
from distinct_dim_temp
group by `real_cust_id`
    grouping sets (
    ( `real_cust_id`)
    )
having 1 = 1
   and (`real_cust_id` is not null);

-- step=4
-- source=temp
-- target=temp
--  tableName=test_cust_case_3__f1e256b9
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `real_cust_id` as `id`,
       `real_cust_code` as `code`,
       `real_cust_bz_time` as `bz_time`,
       '1'                       as `is_auto_create`,
       '${EFFECTIVE_START_TIME}' as `effective_start_time`,
       '9999-01-01 00:00:00'     as `effective_end_time`,
       '1'                       as `is_active`,
       '1'                       as `is_latest`,
       '${DATA_RANGE_START}'     as `idempotent_key`,
       '${DATE_END}'             as `dw_insert_date`
from `ce47db66` `ce47db66`
where 1 = 1
  and (`real_cust_id` is not null)
  and `distinct_count_num` = 1;

-- step=5
-- source=hive
--  dbName=usecase_dwd
--  tableName=test_cust_case_3
-- target=temp
--  tableName=`8b936862`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `(dw_insert_date)?+.+`,
       '${DATE_END}' as `dw_insert_date`
from `usecase_dwd`.`test_cust_case_3`;

-- step=6
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.ZipTableTransformer
--  methodName=transform
--   dwDataLoadType=incremental
--   sortFields=bz_time
--   odsViewName=test_cust_case_3__f1e256b9
--   dwViewName=`8b936862`
--   primaryFields=id
--  transformerType=object
-- target=hive
--  dbName=usecase_dwd
--  tableName=test_cust_case_3
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=overwrite

-- step=7
-- source=temp
-- target=temp
--  tableName=test_fact_target_case_3__6637c70e
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
with duplicate_dimension_temp as (
    select *
    from `ce47db66` `ce47db66`
    where 1 = 1
      and `distinct_count_num` > 1
)
select `test_fact_case_3`.`id` as `id`,
       `test_fact_case_3`.`bz_time` as `bz_time`,
       (case
            when `duplicate_dimension_temp_0`.`real_cust_id` is not null
                then '-99'
            else IFNULL(`test_cust_case_3`.`id`, '-1')
           end) as `real_cust_id`,
       '${EFFECTIVE_START_TIME}' as effective_start_time,
       '9999-01-01 00:00:00'     as effective_end_time,
       '1'                       as is_active,
       '1'                       as is_latest,
       '${DATA_RANGE_START}'     as idempotent_key,
       '${DATE_END}'             as dw_insert_date
from `a8cc8c22` `test_fact_case_3`
         left join `usecase_dwd`.`test_cust_case_3` `test_cust_case_3`
                   on `test_fact_case_3`.`real_cust_id` = `test_cust_case_3`.`id` and `test_cust_case_3`.is_latest = '1'
         left join `duplicate_dimension_temp` `duplicate_dimension_temp_0`
                   on `test_fact_case_3`.`real_cust_id` = `duplicate_dimension_temp_0`.`real_cust_id`;

-- step=8
-- source=temp
-- target=variables
-- checkPoint=false
-- dateRangeInterval=0
select '' as `DW_PARTITION_CLAUSE`;

-- step=9
-- source=hive
--  dbName=usecase_dwd
--  tableName=test_fact_target_case_3
-- target=temp
--  tableName=`1a280f2f`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `(dw_insert_date)?+.+`,
       '${DATE_END}' as `dw_insert_date`
from `usecase_dwd`.`test_fact_target_case_3` `test_fact_target_case_3`
    ${DW_PARTITION_CLAUSE};

-- step=10
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.ZipTableTransformer
--  methodName=transform
--   dwDataLoadType=incremental
--   sortFields=bz_time
--   odsViewName=test_fact_target_case_3__6637c70e
--   dwViewName=`1a280f2f`
--   primaryFields=id
--  transformerType=object
-- target=hive
--  dbName=usecase_dwd
--  tableName=test_fact_target_case_3
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=overwrite
```

</TabItem>
</Tabs>

## Case 4
In Case4, these 2 columns: area_code and area_name, are composite keys, to generate a new primary key (area_id) for the dim table.

<Tabs
defaultValue="markdown"
values={[
{ label: 'Excel', value: 'markdown', },
{ label: 'SQL', value: 'sql', }
]}>

<TabItem value="markdown">

| source_db_name | source_table_name | Source column name | incremental_type   | target_db_name | target_table_name | Target column_name | sort_column | id_column | quality_check_rules | dim_key | dim_sort_column | dim_description | auto_create_dim | auto_create_dim_id | dim_db_name | dim_table_name | dim_column_name | zip_dim_key | partition_column 
| :------------- | :---------------- | :----------------- | :----------------- | :------------- | :---------------- | :----------------- | :---------- | :-------- | :------------------ | :------ | :-------------- | :-------------- | :-------------- | :----------------- | :---------- | :------------- | :-------------- | :------------- | :-------------- |
| uesecase_ods        | test_fact_auto_dim         | id                 | incremental_append | usecase_dwd        | test_fact_target_auto_dim  | id                 |             | TRUE      |                     |         |                 |                 |                 |                    |             |                |                 |                |                 
| uesecase_ods        | test_fact_auto_dim         |                    | incremental_append | usecase_dwd        | test_fact_target_auto_dim  | area_id            |             |           |                     |         |                 |                 | TRUE            | TRUE               | usecase_dwd     | test_area      | id              |                |                 
| uesecase_ods        | test_fact_auto_dim         | area_code          | incremental_append | usecase_dwd        | test_fact_target_auto_dim  | area_code          |             |           |                     | TRUE    |                 |                 | TRUE            |                    | usecase_dwd     | test_area      | area_cd         |                |                 
| uesecase_ods        | test_fact_auto_dim         | area_name          | incremental_append | usecase_dwd        | test_fact_target_auto_dim  | area_name          |             |           |                     | TRUE    |                 |                 | TRUE            |                    | usecase_dwd     | test_area      | area_nm         |                |                 
| uesecase_ods        | test_fact_auto_dim         | area_bz_time       | incremental_append | usecase_dwd        | test_fact_target_auto_dim  | area_bz_time       |             |           |                     |         | TRUE            | TRUE            | TRUE            |                    | usecase_dwd     | test_area      | bz_time         |                |                 
| uesecase_ods        | test_fact_auto_dim         | bz_time            | incremental_append | usecase_dwd        | test_fact_target_auto_dim  | bz_time            | TRUE        |           |                     |         |                 |                 |                 |                    |             |                |                 |                |                 

</TabItem>

<TabItem value="sql">

After running the same command, the sql file corresponding to Case4 named `default_test_fact_target.sql` will be generated on your desktop:


```sql
-- step=1
-- source=temp
-- target=variables
-- checkPoint=false
-- dateRangeInterval=0
select from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMdd')               as `DATE_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'HH')                     as `HOUR_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')  as `EFFECTIVE_START_TIME`;

-- step=2
-- source=hive
--  dbName=usecase_ods
--  tableName=test_fact_auto_dim
--  options
--   idColumn=id
-- target=temp
--  tableName=`634737e2`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select *
from `usecase_ods`.`test_fact_auto_dim`;

-- step=3
-- source=temp
-- target=temp
--  tableName=`cb939fe5`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
with join_fact_temp as (select nullif(`test_fact_auto_dim`.`area_code`, `test_area`.`area_cd`) as `area_code`,
                               nullif(`test_fact_auto_dim`.`area_name`, `test_area`.`area_nm`) as `area_name`,
                               nullif(`test_fact_auto_dim`.`area_bz_time`, `test_area`.`bz_time`) as `area_bz_time`
                        from `634737e2` `test_fact_auto_dim`
                                 left join `usecase_dwd`.`test_area` `test_area`
                                           on `test_fact_auto_dim`.`area_code` = `test_area`.`area_cd`
                                               and `test_fact_auto_dim`.`area_name` = `test_area`.`area_nm`
                                               and `test_area`.`is_latest` = '1'),
     distinct_dim_temp as (select `area_code`,
                                  `area_name`,
                                  `area_bz_time`
                           from join_fact_temp
                           where 1 = 1
                             and (`area_code` is not null and `area_name` is not null)
                           group by `area_code`, `area_name`, `area_bz_time`
    grouping sets (
    ( `area_code`, `area_name`, `area_bz_time`)
    ))
select `area_code` as `area_code`,
       `area_name` as `area_name`,
       first_value(`area_bz_time`) as `area_bz_time`,
       count(1) as `distinct_count_num`
from distinct_dim_temp
group by `area_code`, `area_name`
    grouping sets (
    ( `area_code`, `area_name`)
    )
having 1 = 1
   and (`area_code` is not null and `area_name` is not null);

-- step=4
-- source=temp
-- target=temp
--  tableName=test_area__f5045d67
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select uuid() as `id`,
       `area_code` as `area_cd`,
       `area_name` as `area_nm`,
       `area_bz_time` as `bz_time`,
       '1'                       as `is_auto_create`,
       '${EFFECTIVE_START_TIME}' as `effective_start_time`,
       '9999-01-01 00:00:00'     as `effective_end_time`,
       '1'                       as `is_active`,
       '1'                       as `is_latest`,
       '${DATA_RANGE_START}'     as `idempotent_key`,
       '${DATE_END}'             as `dw_insert_date`
from `cb939fe5` `cb939fe5`
where 1 = 1
  and (`area_code` is not null       and
       `area_name` is not null)
  and `distinct_count_num` = 1;

-- step=5
-- source=hive
--  dbName=usecase_dwd
--  tableName=test_area
-- target=temp
--  tableName=`8878baea`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `(dw_insert_date)?+.+`,
       '${DATE_END}' as `dw_insert_date`
from `usecase_dwd`.`test_area`
where `dw_insert_date` = (select max(`dw_insert_date`)
                          from `usecase_dwd`.`test_area`);

-- step=6
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.ZipTableTransformer
--  methodName=transform
--   dwDataLoadType=incremental
--   sortFields=bz_time
--   odsViewName=test_area__f5045d67
--   dwViewName=`8878baea`
--   primaryFields=area_cd,area_nm
--  transformerType=object
-- target=hive
--  dbName=usecase_dwd
--  tableName=test_area
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=overwrite

-- step=7
-- source=temp
-- target=temp
--  tableName=test_fact_target_auto_dim__d3531bfc
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
with duplicate_dimension_temp as (
    select *
    from `cb939fe5` `cb939fe5`
    where 1 = 1
      and `distinct_count_num` > 1
)
select `test_fact_auto_dim`.`id` as `id`,
       `test_fact_auto_dim`.`bz_time` as `bz_time`,
       (case
            when `duplicate_dimension_temp_0`.`area_code` is not null and `duplicate_dimension_temp_0`.`area_name` is not null
                then '-99'
            else IFNULL(`test_area`.`id`, '-1')
           end) as ``,
       '${EFFECTIVE_START_TIME}' as effective_start_time,
       '9999-01-01 00:00:00'     as effective_end_time,
       '1'                       as is_active,
       '1'                       as is_latest,
       '${DATA_RANGE_START}'     as idempotent_key,
       '${DATE_END}'             as dw_insert_date
from `634737e2` `test_fact_auto_dim`
         left join `usecase_dwd`.`test_area` `test_area`
                   on `test_fact_auto_dim`.`area_code` = `test_area`.`area_cd`
                       and `test_fact_auto_dim`.`area_name` = `test_area`.`area_nm` and `test_area`.is_latest = '1'
         left join `duplicate_dimension_temp` `duplicate_dimension_temp_0`
                   on `test_fact_auto_dim`.`area_code` = `duplicate_dimension_temp_0`.`area_code`
                       and `test_fact_auto_dim`.`area_name` = `duplicate_dimension_temp_0`.`area_name`;

-- step=8
-- source=hive
--  dbName=usecase_dwd
--  tableName=test_fact_target_auto_dim
-- target=temp
--  tableName=`a3652e87`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `(dw_insert_date)?+.+`,
       '${DATE_END}' as `dw_insert_date`
from `usecase_dwd`.`test_fact_target_auto_dim` `test_fact_target_auto_dim`
where `dw_insert_date` = (select max(`dw_insert_date`)
                          from `usecase_dwd`.`test_fact_target_auto_dim` `test_fact_target_auto_dim`);

-- step=9
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.ZipTableTransformer
--  methodName=transform
--   dwDataLoadType=incremental
--   sortFields=bz_time
--   odsViewName=`634737e2`
--   dwViewName=`a3652e87`
--   primaryFields=id
--  transformerType=object
-- target=hive
--  dbName=usecase_dwd
--  tableName=test_fact_target_auto_dim
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=overwrite

```
</TabItem>
</Tabs>

## Case 5

if origin data has no id column

not supported for now.


## Case 6

In Case 6, the origin data has no business time column.

<Tabs
defaultValue="markdown"
values={[
{ label: 'Excel', value: 'markdown', },
{ label: 'SQL', value: 'sql', },
{ label: 'ODS SQL', value: 'ods', },
{ label: 'DWD SQL', value: 'dwd', }
]}>

<TabItem value="markdown">

| source_db_name | source_table_name | column_name | incremental_type   | target_db_name | target_table_name | target_column_name | expression | sort_column | id_column | quality_check_rules | dim_key | dim_sort_column | dim_description | auto_create_dim | auto_create_dim_id | dim_db_name | dim_table_name | dim_column_name | zip_dim_key | partition_column|
| :------------- | :---------------- | :---------- | :----------------- | :------------- | :---------------- | :----------------- | :--------- | :---------- | :-------- | :------------------ | :------ | :-------------- | :-------------- | :-------------- | :----------------- | :---------- | :------------- | :-------------- | :---------- | :--------------|
| usecase_ods    | test_fact_case_6         | id          | incremental_append | usecase_dwd    | test_fact_target_case_6  | id                 |  TRUE      |   TRUE      |           |                     |         |                 |                 |                 |                    |             |                       |                 |             |
| usecase_ods    | test_fact_case_6         |             | incremental_append | usecase_dwd    | test_fact_target_case_6  | area_id            |            |             |           |                     |   TRUE  |     TRUE        |                 | TRUE            |                    | usecase_dwd | test_area_case_6      | id              |             |
| usecase_ods    | test_fact_case_6         | area_code   | incremental_append | usecase_dwd    | test_fact_target_case_6  | area_code          |            |             |           |                     |         |                 |        TRUE     | TRUE            |                    | usecase_dwd | test_area_case_6      | area_cd         |             |
| usecase_ods    | test_fact_case_6         | area_name   | incremental_append | usecase_dwd    | test_fact_target_case_6  | area_name          |            |             |           |                     |         |                 |        TRUE     | TRUE            |                    | usecase_dwd | test_area_case_6      | area_nm         |             |

</TabItem>

<TabItem value="sql">

After running the same command, the sql file `default_test_fact_target.sql` will be generated:

```sql
-- step=1
-- source=temp
-- target=variables
-- checkPoint=false
-- dateRangeInterval=0
select from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMdd')               as `DATE_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'HH')                     as `HOUR_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')  as `EFFECTIVE_START_TIME`;

-- step=2
-- source=hive
--  dbName=usecase_ods
--  tableName=test_fact_case_6
--  options
--   idColumn=id
-- target=temp
--  tableName=`b53c9449`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select *
from `usecase_ods`.`test_fact_case_6`;

-- step=3
-- source=temp
-- target=temp
--  tableName=`6954aac5`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
with join_fact_temp as (select nullif(`test_fact_case_6`.`area_id`, `test_area_case_6`.`id`) as `area_id`,
                               nullif(`test_fact_case_6`.`area_code`, `test_area_case_6`.`area_cd`) as `area_code`,
                               nullif(`test_fact_case_6`.`area_name`, `test_area_case_6`.`area_nm`) as `area_name`
                        from `b53c9449` `test_fact_case_6`
                                 left join `usecase_dwd`.`test_area_case_6` `test_area_case_6`
                                           on `test_fact_case_6`.`area_id` = `test_area_case_6`.`id`
                                               and `test_area_case_6`.`is_latest` = '1'),
     distinct_dim_temp as (select `area_id`,
                                  `area_code`,
                                  `area_name`
                           from join_fact_temp
                           where 1 = 1
                             and (`area_id` is not null)
                           group by `area_id`, `area_code`, `area_name`
    grouping sets (
    ( `area_id`, `area_code`, `area_name`)
    ))
select `area_id` as `area_id`,
       first_value(`area_code`) as `area_code`,
       first_value(`area_name`) as `area_name`,
       count(1) as `distinct_count_num`
from distinct_dim_temp
group by `area_id`
    grouping sets (
    ( `area_id`)
    )
having 1 = 1
   and (`area_id` is not null);

-- step=4
-- source=temp
-- target=temp
--  tableName=test_area_case_6__ebc46690
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `area_id` as `id`,
       `area_code` as `area_cd`,
       `area_name` as `area_nm`,
       '1'                       as `is_auto_create`,
       '${EFFECTIVE_START_TIME}' as `effective_start_time`,
       '9999-01-01 00:00:00'     as `effective_end_time`,
       '1'                       as `is_active`,
       '1'                       as `is_latest`,
       '${DATA_RANGE_START}'     as `idempotent_key`,
       '${DATE_END}'             as `dw_insert_date`
from `6954aac5` `6954aac5`
where 1 = 1
  and (`area_id` is not null)
  and `distinct_count_num` = 1;

-- step=5
-- source=hive
--  dbName=usecase_dwd
--  tableName=test_area_case_6
-- target=temp
--  tableName=`1c7bcb60`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `(dw_insert_date)?+.+`,
       '${DATE_END}' as `dw_insert_date`
from `usecase_dwd`.`test_area_case_6`;

-- step=6
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.ZipTableTransformer
--  methodName=transform
--   dwDataLoadType=incremental
--   sortFields=id
--   odsViewName=test_area_case_6__ebc46690
--   dwViewName=`1c7bcb60`
--   primaryFields=id
--  transformerType=object
-- target=hive
--  dbName=usecase_dwd
--  tableName=test_area_case_6
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=overwrite

-- step=7
-- source=temp
-- target=temp
--  tableName=test_fact_target_case_6__e9ae24c3
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
with duplicate_dimension_temp as (
    select *
    from `6954aac5` `6954aac5`
    where 1 = 1
      and `distinct_count_num` > 1
)
select `test_fact_case_6`.`id` as `id`,
       (case
            when `duplicate_dimension_temp_0`.`area_id` is not null
                then '-99'
            else IFNULL(`test_area_case_6`.`id`, '-1')
           end) as `area_id`,
       '${EFFECTIVE_START_TIME}' as effective_start_time,
       '9999-01-01 00:00:00'     as effective_end_time,
       '1'                       as is_active,
       '1'                       as is_latest,
       '${DATA_RANGE_START}'     as idempotent_key,
       '${DATE_END}'             as dw_insert_date
from `b53c9449` `test_fact_case_6`
         left join `usecase_dwd`.`test_area_case_6` `test_area_case_6`
                   on `test_fact_case_6`.`area_id` = `test_area_case_6`.`id` and `test_area_case_6`.is_latest = '1'
         left join `duplicate_dimension_temp` `duplicate_dimension_temp_0`
                   on `test_fact_case_6`.`area_id` = `duplicate_dimension_temp_0`.`area_id`;

-- step=8
-- source=temp
-- target=variables
-- checkPoint=false
-- dateRangeInterval=0
select '' as `DW_PARTITION_CLAUSE`;

-- step=9
-- source=hive
--  dbName=usecase_dwd
--  tableName=test_fact_target_case_6
-- target=temp
--  tableName=`775d93f3`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `(dw_insert_date)?+.+`,
       '${DATE_END}' as `dw_insert_date`
from `usecase_dwd`.`test_fact_target_case_6` `test_fact_target_case_6`
    ${DW_PARTITION_CLAUSE};

-- step=10
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.ZipTableTransformer
--  methodName=transform
--   dwDataLoadType=incremental
--   sortFields=id
--   odsViewName=test_fact_target_case_6__e9ae24c3
--   dwViewName=`775d93f3`
--   primaryFields=id
--  transformerType=object
-- target=hive
--  dbName=usecase_dwd
--  tableName=test_fact_target_case_6
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=overwrite
```

</TabItem>

<TabItem value="ods">
Script for creating ODS tables: 


```sql
create table usecase_ods.test_fact
(
    id          varchar(255),
    area_code   varchar(255),    
    area_name   varchar(255),    
    job_time    timestamp
)
```

Insert data to ODS: 

```sql
insert into usecase_ods.test_fact values(1, '123', 'area-123', '2020-01-01 15:05:05');
insert into usecase_ods.test_fact values(2, '456', 'area-456', '2020-11-01 15:05:05');
```

</TabItem>

<TabItem value="dwd">
Script for creating DWD tables.

```sql
create table usecase_dwd.test_fact_target
(
    id                      varchar(255),
    area_id                 varchar(255),    
    job_time                timestamp,
    is_auto_create          varchar(255),
    effective_start_time    timestamp,
    effective_end_time      timestamp,
    is_active               varchar(255),
    is_latest               varchar(255),
    idempotent_key          varchar(255),
    dw_insert_date          varchar(255)
);

create table usecase_dwd.test_area
(
    id          varchar(255),
    area_cd     varchar(255),    
    area_nm     varchar(255),    
    bz_time     timestamp,
    is_auto_create          varchar(255),
    effective_start_time    timestamp,
    effective_end_time      timestamp,
    is_active               varchar(255),
    is_latest               varchar(255),
    idempotent_key          varchar(255),
    dw_insert_date          varchar(255)
);

```
</TabItem>

</Tabs>

## Case 7

if origin system do HARD delete

incremental_type = incremental_diff

## Case 8

In Case8, the auto-created dim table combine from multiple fact table

<Tabs
defaultValue="markdown"
values={[
{ label: 'Excel', value: 'markdown', },
{ label: 'SQL', value: 'sql', },
{ label: 'ODS SQL', value: 'ods', },
{ label: 'DWD SQL', value: 'dwd', }
]}>

<TabItem value="markdown">

| source_db_name | source_table_name  | column_name    | incremental_type   | target_db_name | target_table_name      | target_column_name | expression | sort_column | id_column | quality_check_rules | dim_key | dim_sort_column | dim_description | auto_create_dim | auto_create_dim_id | dim_db_name | dim_table_name | dim_column_name | zip_dim_key | partition_column |
| :------------- | :----------------- | :------------- | :----------------- | :------------- | :--------------------- | :----------------- | :--------- | :---------- | :-------- | :------------------ | :------ | :-------------- | :-------------- | :-------------- | :----------------- | :---------- | :------------- | :-------------- | :---------- | :--------------- |
| usecase_ods    | test_fact_case8    | id             | incremental_append | usecase_dwd    | test_fact_target_case8 | id                 |            |             | TRUE      |                     |         |                 |                 |                 |                    |             |                |                 |             |                  |
| usecase_ods    | test_fact_case8    | region_id      | incremental_append | usecase_dwd    | test_fact_target_case8 | region_id          |            |             |           |                     | TRUE    |                 |                 | TRUE            |                    | usecase_dwd | test_region    | id              |             |                  |
| usecase_ods    | test_fact_case8    | region_code    | incremental_append | usecase_dwd    | test_fact_target_case8 | region_code        |            |             |           |                     |         |                 | TRUE            | TRUE            |                    | usecase_dwd | test_region    | region_cd       |             |                  |
| usecase_ods    | test_fact_case8    | region_name    | incremental_append | usecase_dwd    | test_fact_target_case8 | region_name        |            |             |           |                     |         |                 | TRUE            | TRUE            |                    | usecase_dwd | test_region    | region_nm       |             |                  |
| usecase_ods    | test_fact_case8    | region_bz_time | incremental_append | usecase_dwd    | test_fact_target_case8 | region_bz_time     |            |             |           |                     |         | TRUE            | TRUE            | TRUE            |                    | usecase_dwd | test_region    | bz_time         |             |                  |
| usecase_ods    | test_fact_case8    | bz_time        | incremental_append | usecase_dwd    | test_fact_target_case8 | bz_time            |            | TRUE        |           |                     |         |                 |                 |                 |                    |             |                |                 |             |                  |
| usecase_ods    | test_store_fact    | id             | incremental_append | usecase_dwd    | test_store_fact_target | id                 |            |             | TRUE      |                     |         |                 |                 |                 |                    |             |                |                 |             |                  |
| usecase_ods    | test_store_fact    | region_id      | incremental_append | usecase_dwd    | test_store_fact_target | region_id          |            |             |           |                     | TRUE    |                 |                 | TRUE            |                    | usecase_dwd | test_region    | id              |             |                  |
| usecase_ods    | test_store_fact    | region_count   | incremental_append | usecase_dwd    | test_store_fact_target | region_count       |            |             |           |                     |         |                 | TRUE            | TRUE            |                    | usecase_dwd | test_region    | region_ct       |             |                  |
| usecase_ods    | test_store_fact    | region_address | incremental_append | usecase_dwd    | test_store_fact_target | region_address     |            |             |           |                     |         |                 | TRUE            | TRUE            |                    | usecase_dwd | test_region    | region_address  |             |                  |
| usecase_ods    | test_store_fact    | region_bz_time | incremental_append | usecase_dwd    | test_store_fact_target | region_bz_time     |            |             |           |                     |         | TRUE            | TRUE            | TRUE            |                    | usecase_dwd | test_region    | bz_time         |             |                  |
| usecase_ods    | test_store_fact    | bz_time        | incremental_append | usecase_dwd    | test_store_fact_target | bz_time            |            | TRUE        |           |                     |         |                 |                 |                 |                    |             |                |                 |             |                  |
| usecase_ods    | test_region_source | id             | incremental_append | usecase_dwd    | test_region            | id                 |            |             | TRUE      |                     |         |                 |                 |                 |                    |             |                |                 |             |                  |
| usecase_ods    | test_region_source | region_cd      | incremental_append | usecase_dwd    | test_region            | region_cd          |            |             |           |                     |         |                 |                 |                 |                    |             |                |                 |             |                  |
| usecase_ods    | test_region_source | region_nm      | incremental_append | usecase_dwd    | test_region            | region_nm          |            |             |           |                     |         |                 |                 |                 |                    |             |                |                 |             |                  |
| usecase_ods    | test_region_source | region_ct      | incremental_append | usecase_dwd    | test_region            | region_ct          |            |             |           |                     |         |                 |                 |                 |                    |             |                |                 |             |                  |
| usecase_ods    | test_region_source | region_address | incremental_append | usecase_dwd    | test_region            | region_address     |            |             |           |                     |         |                 |                 |                 |                    |             |                |                 |             |                  |
| usecase_ods    | test_region_source | bz_time        | incremental_append | usecase_dwd    | test_region            | bz_time            |            | TRUE        |           |                     |         |                 |                 |                 |                    |             |                |                 |             |                  |
| usecase_ods    | test_region_source |                | incremental_append | usecase_dwd    | test_region            | is_auto_create     | '0'        |             |           |                     |         |                 |                 |                 |                    |             |                |                 |             |                  |											

</TabItem>


<TabItem value="sql">

There will be 3 sql files generated on your desktop, named `usecase_dwd_test_region.sql`, `usecase_dwd_test_fact_target_case8.sql` and `usecase_dwd_test_store_fact_target.sql`, respectively.

`usecase_dwd_test_region.sql`:

```sql
-- step=1
-- source=temp
-- target=variables
-- checkPoint=false
-- dateRangeInterval=0
select from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMdd')               as `DATE_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'HH')                     as `HOUR_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')  as `EFFECTIVE_START_TIME`;

-- step=2
-- source=hive
--  dbName=usecase_ods
--  tableName=test_region_source
--  options
--   idColumn=id
-- target=temp
--  tableName=`b84d02c6`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `id` as `id`,
       `region_cd` as `region_cd`,
       `region_nm` as `region_nm`,
       `region_ct` as `region_ct`,
       `region_address` as `region_address`,
       `bz_time` as `bz_time`,
       '0' as `is_auto_create`,
       '${EFFECTIVE_START_TIME}' as effective_start_time,
       '9999-01-01 00:00:00'     as effective_end_time,
       '1'                       as is_active,
       '1'                       as is_latest,
       '${DATA_RANGE_START}'     as idempotent_key,
       '${DATE_END}'             as dw_insert_date
from `usecase_ods`.`test_region_source`;

-- step=3
-- source=hive
--  dbName=usecase_dwd
--  tableName=test_region
-- target=temp
--  tableName=`eac5375c`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `(dw_insert_date)?+.+`,
       '${DATE_END}' as `dw_insert_date`
from `usecase_dwd`.`test_region` `test_region`
where `dw_insert_date` = (select max(`dw_insert_date`)
                          from `usecase_dwd`.`test_region` `test_region`);

-- step=4
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.ZipTableTransformer
--  methodName=transform
--   dwDataLoadType=incremental
--   sortFields=bz_time
--   odsViewName=`b84d02c6`
--   dwViewName=`eac5375c`
--   primaryFields=id
--  transformerType=object
-- target=hive
--  dbName=usecase_dwd
--  tableName=test_region
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=overwrite

```

`usecase_dwd_test_fact_target_case8.sql`:

```sql
-- step=1
-- source=temp
-- target=variables
-- checkPoint=false
-- dateRangeInterval=0
select from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMdd')               as `DATE_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'HH')                     as `HOUR_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')  as `EFFECTIVE_START_TIME`;

-- step=2
-- source=hive
--  dbName=usecase_ods
--  tableName=test_fact_case8
--  options
--   idColumn=id
-- target=temp
--  tableName=`c7a033fe`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `test_fact_case8`.`id` as `id`,
       `test_fact_case8`.`bz_time` as `bz_time`,
       ifnull(`test_region`.`id`, '-1') as `region_id`,
       '${EFFECTIVE_START_TIME}' as effective_start_time,
       '9999-01-01 00:00:00'     as effective_end_time,
       '1'                       as is_active,
       '1'                       as is_latest,
       '${DATA_RANGE_START}'     as idempotent_key,
       '${DATE_END}'             as dw_insert_date
from `usecase_ods`.`test_fact_case8` `test_fact_case8`
         left join `usecase_dwd`.`test_region` `test_region`
                   on `test_fact_case8`.`region_id` = `test_region`.`id` and `test_region`.is_latest = '1';

-- step=3
-- source=hive
--  dbName=usecase_dwd
--  tableName=test_fact_target_case8
-- target=temp
--  tableName=`b99ce421`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `(dw_insert_date)?+.+`,
       '${DATE_END}' as `dw_insert_date`
from `usecase_dwd`.`test_fact_target_case8` `test_fact_target_case8`
where `dw_insert_date` = (select max(`dw_insert_date`)
                          from `usecase_dwd`.`test_fact_target_case8` `test_fact_target_case8`);

-- step=4
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.ZipTableTransformer
--  methodName=transform
--   dwDataLoadType=incremental
--   sortFields=bz_time
--   odsViewName=`c7a033fe`
--   dwViewName=`b99ce421`
--   primaryFields=id
--  transformerType=object
-- target=hive
--  dbName=usecase_dwd
--  tableName=test_fact_target_case8
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=overwrite

```

`usecase_dwd_test_store_fact_target.sql`:

```sql
-- step=1
-- source=temp
-- target=variables
-- checkPoint=false
-- dateRangeInterval=0
select from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'yyyyMMdd')               as `DATE_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_END}', 'yyyy-MM-dd HH:mm:ss'), 'HH')                     as `HOUR_END`,
       from_unixtime(unix_timestamp('${DATA_RANGE_START}', 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')  as `EFFECTIVE_START_TIME`;

-- step=2
-- source=hive
--  dbName=usecase_ods
--  tableName=test_store_fact
--  options
--   idColumn=id
-- target=temp
--  tableName=`ee9d6f1e`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `test_store_fact`.`id` as `id`,
       `test_store_fact`.`bz_time` as `bz_time`,
       ifnull(`test_region`.`id`, '-1') as `region_id`,
       '${EFFECTIVE_START_TIME}' as effective_start_time,
       '9999-01-01 00:00:00'     as effective_end_time,
       '1'                       as is_active,
       '1'                       as is_latest,
       '${DATA_RANGE_START}'     as idempotent_key,
       '${DATE_END}'             as dw_insert_date
from `usecase_ods`.`test_store_fact` `test_store_fact`
         left join `usecase_dwd`.`test_region` `test_region`
                   on `test_store_fact`.`region_id` = `test_region`.`id` and `test_region`.is_latest = '1';

-- step=3
-- source=hive
--  dbName=usecase_dwd
--  tableName=test_store_fact_target
-- target=temp
--  tableName=`ff1acfcc`
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
select `(dw_insert_date)?+.+`,
       '${DATE_END}' as `dw_insert_date`
from `usecase_dwd`.`test_store_fact_target` `test_store_fact_target`
where `dw_insert_date` = (select max(`dw_insert_date`)
                          from `usecase_dwd`.`test_store_fact_target` `test_store_fact_target`);

-- step=4
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.ZipTableTransformer
--  methodName=transform
--   dwDataLoadType=incremental
--   sortFields=bz_time
--   odsViewName=`ee9d6f1e`
--   dwViewName=`ff1acfcc`
--   primaryFields=id
--  transformerType=object
-- target=hive
--  dbName=usecase_dwd
--  tableName=test_store_fact_target
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=overwrite


```

</TabItem>

<TabItem value="ods">
Script for creating ODS tables: 


```sql
create table usecase_ods.test_fact_case8
(
    id              varchar(255),
    region_id       varchar(255),
    region_code       varchar(255),    
    region_name       varchar(255), 
    region_bz_time    timestamp,
    bz_time         timestamp,
		job_id          varchar(255),
    job_time        timestamp
);

create table usecase_ods.test_store_fact
(
    id              varchar(255),
    region_id       varchar(255),
    region_count      varchar(255),    
    region_address    varchar(255), 
    region_bz_time    timestamp,
    bz_time         timestamp,
    job_id          varchar(255),
    job_time        timestamp
);

create table usecase_ods.test_region_source
(
    id              varchar(255),
    region_cd         varchar(255),    
    region_nm         varchar(255), 
    region_ct         int,
    region_address    varchar(255),
    bz_time         timestamp,
    job_id          varchar(255),
    job_time        timestamp
);
```

Insert data to ODS: 

```sql
insert into usecase_ods.test_fact_case8 values(1, '1', '123', 'region-123',  '2020-01-01 15:05:05', '2020-01-01 15:05:05', '1', '2020-01-01 15:05:05');
insert into usecase_ods.test_fact_case8 values(2, '2', '456', 'region-456',  '2020-11-01 15:05:05', '2020-11-01 15:05:05', '2', '2020-11-01 15:05:05');

insert into usecase_ods.test_store_fact values(1, '1', 123, 'address-123', '2020-01-01 15:05:05', '2020-01-01 15:05:05', '1', '2020-01-01 15:05:05');
insert into usecase_ods.test_store_fact values(2, '2', 456, 'address-456', '2020-11-01 15:05:05', '2020-11-01 15:05:05', '2', '2020-11-01 15:05:05');

insert into usecase_ods.test_region_source values(1, '123', 'region-123', 123, 'address-123', '2020-01-01 15:05:05', '1', '2020-01-01 15:05:05');
insert into usecase_ods.test_region_source values(2, '456', 'region-456', 456, 'address-456', '2020-11-01 15:05:05', '2', '2020-11-01 15:05:05');
```

</TabItem>

<TabItem value="dwd">
Script for creating DWD tables.

```sql
create table usecase_dwd.test_fact_target_case8
(
    id                      varchar(255),
    region_id                 varchar(255),    
    bz_time                 timestamp,
    job_time                timestamp,
    effective_start_time    timestamp,
    effective_end_time      timestamp,
    is_active               varchar(255),
    is_latest               varchar(255),
    idempotent_key          varchar(255),
    dw_insert_date          varchar(255)
);

create table usecase_dwd.test_store_fact_target
(
    id               varchar(255), 
    region_id        varchar(255),                 
    bz_time          timestamp,
		job_time                timestamp,
    effective_start_time    timestamp,
    effective_end_time      timestamp,
    is_active               varchar(255),
    is_latest               varchar(255),
    idempotent_key          varchar(255),
    dw_insert_date          varchar(255)
);

create table usecase_dwd.test_region
(
    id                      varchar(255),
    region_cd                 varchar(255),    
    region_nm                 varchar(255),    
    region_ct                 int,
    region_address            varchar(255),
    bz_time                 timestamp,
    job_time                timestamp,
    is_auto_create          varchar(255),
    effective_start_time    timestamp,
    effective_end_time      timestamp,
    is_active               varchar(255),
    is_latest               varchar(255),
    idempotent_key          varchar(255),
    dw_insert_date          varchar(255)
);
```

> Please Note: field `is_auto_create` is required for **Dim** table. It is used to mark whether this is from source data or generated by ETL framework.

</TabItem>


</Tabs>
