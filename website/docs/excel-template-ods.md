---
title: "Excel template for source to ods"
sidebar_position: 3
toc: true
last_modified_at: 2021-10-21T10:59:57-04:00
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## pre-requirements

* all tables must exist before running ETL jobs(ETL doesn't create table)

* you can download [this excel](https://docs.google.com/spreadsheets/d/1Prw1LFfkSkaAuf1K6O0TTI5PPRP7lLtzIR63x9HCSVw/edit#gid=1642393109) to your `~/Desktop` for the quick start guide.
## config sample

<Tabs
defaultValue="markdown"
values={[
{ label: 'Excel', value: 'markdown', },
{ label: 'SQL', value: 'sql', }
]}>

<TabItem value="markdown">

| source_db_name | source_table_name | source_column_name | is_PK | incremental_column | additional_filter | target_db_name | target_table_name | target_column_name | expression | incremental_type   | partition column | update_frequency |
| :------------- | :---------------- | :----------------- | :---- | :----------------- | :---------------- | :------------- | :---------------- | :----------------- | :--------- | :----------------- | :--------------- | :--------------- |
| db_name        | table_name        | xx1                | 1     |                    |                   | db_name        | ods_table_name    | xx1                |            | incremental_append |                  | 1440             |
| db_name        | table_name        | xx2                |       |                    |                   | db_name        | ods_table_name    | xx2                |            | incremental_append |                  | 1440             |
| db_name        | table_name        | xx3                |       |                    |                   | db_name        | ods_table_name    | xx3                |            | incremental_append |                  | 1440             |
| db_name        | table_name        | xx4                |       |                    |                   | db_name        | ods_table_name    | xx4                |            | incremental_append |                  | 1440             |
| db_name        | table_name        | xx5                |       |                    |                   | db_name        | ods_table_name    | xx5                |            | incremental_append |                  | 1440             |
| db_name        | table_name        | xx6                |       |                    |                   | db_name        | ods_table_name    | xx6                |            | incremental_append |                  | 1440             |
| db_name        | table_name        | xx7                |       |                    |                   | db_name        | ods_table_name    | xx7                |            | incremental_append |                  | 1440             |
| db_name        | table_name        | xx8                |       |                    |                   | db_name        | ods_table_name    | xx8                |            | incremental_append |                  | 1440             |
| db_name        | table_name        | xx9                |       |                    |                   | db_name        | ods_table_name    | xx9                |            | incremental_append |                  | 1440             |
| db_name        | table_name        | xx10               |       | TRUE               |                   | db_name        | ods_table_name    | xx10               |            | incremental_append |                  | 1440             |
| db_name        | table_name        |                    |       |                    |                   | db_name        | ods_table_name    | job_id             | ${JOB_ID}  | incremental_append |                  | 1440             |
| db_name        | table_name        |                    |       |                    |                   | db_name        | ods_table_name    | job_time           | now()      | incremental_append |                  | 1440             |
| db_name        | table_name        |                    |       |                    |                   | db_name        | ods_table_name    | load_dt            | now()      | incremental_append |                  | 1440             |

</TabItem>

<TabItem value="sql">

By running the following command, there is a sql file generated on your desktop:

```bash
./gradlew :spark:run --args="generate-ods-sql -f ~/Desktop/数据字典-模版.xlsx --output ~/Desktop/"
```

Then the .sql file illustrates the steps on how to handle the data from the excel config.

```sql
-- step=1
-- source=hive
--  dbName=db_name
--  tableName=table_name
-- target=hive
--  dbName=db_name
--  tableName=ods_table_name
-- checkPoint=false
-- dateRangeInterval=0
-- writeMode=append
-- incrementalType=incremental_append
SELECT `xx1` AS `xx1`,
       `xx2` AS `xx2`,
       `xx3` AS `xx3`,
       `xx4` AS `xx4`,
       `xx5` AS `xx5`,
       `xx6` AS `xx6`,
       `xx7` AS `xx7`,
       `xx8` AS `xx8`,
       `xx9` AS `xx9`,
       `xx10` AS `xx10`,
       ${JOB_ID} AS `job_id`,
       now() AS `job_time`,
       now() AS `load_dt`
FROM `db_name`.`table_name`
WHERE `xx10` >= '${DATA_RANGE_START}' AND `xx10` < '${DATA_RANGE_END}';
```

</TabItem>

</Tabs>
