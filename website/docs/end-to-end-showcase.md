---
title: "End to end showcase(Hive)"
sidebar_position: 2
toc: true
last_modified_at: 2022-04-09T10:59:57+8:00
---


## 环境准备

* [Docker setup](docker-setup)

```bash
docker run --name postgres -e POSTGRES_PASSWORD=postgres -d -p 5432:5432 postgres:12.0-alpine
```

## 运行任务

### 准备源表

postgres:

```sql
create schema if not exists sales;

create table if not exists sales.order
(
    order_sn          varchar(128),
    product_code      varchar(128),
    product_name      varchar(128),
    product_version   varchar(128),
    product_status    varchar(128),
    user_code         varchar(128),
    user_name         varchar(128),
    user_age          int,
    user_address      varchar(128),
    product_count     int,
    price             decimal(10, 4),
    discount          decimal(10, 4),
    order_status      varchar(128),
    order_create_time timestamp,
    order_update_time timestamp
);

create table if not exists sales.user
(
    user_code    varchar(128),
    user_name    varchar(128),
    user_age     int,
    user_address varchar(128),
    create_time  timestamp,
    update_time  timestamp
);

create table if not exists sales.product
(
    mid         varchar(128),
    name        varchar(128),
    version     varchar(128),
    status      varchar(128),
    create_time timestamp,
    update_time timestamp
);
```


hive ods:

```sql
create database if not exists ods;

create table if not exists ods.t_order
(
    order_sn          string,
    product_code      string,
    product_name      string,
    product_version   string,
    product_status    string,
    user_code         string,
    user_name         string,
    user_age          int,
    user_address      string,
    product_count     int,
    price             decimal(10, 4),
    discount          decimal(10, 4),
    order_status      string,
    order_create_time timestamp,
    order_update_time timestamp,
    job_id            string
) partitioned by (`year` string, `month` string, `day` string);

create table if not exists ods.t_user
(
    user_code    string,
    user_name    string,
    user_age     int,
    user_address string,
    create_time  timestamp,
    update_time  timestamp,
    job_id       string
) partitioned by (`year` string, `month` string, `day` string);

create table if not exists ods.t_product
(
    product_code    string,
    product_name    string,
    product_version string,
    product_status  string,
    create_time     timestamp,
    update_time     timestamp,
    job_id          string
) partitioned by (`year` string, `month` string, `day` string);
```

hive dwd:

```sql
create database if not exists dwd;
create database if not exists dim;
create table dwd.t_fact_order
(
    order_id          string,
    order_sn          string,
    product_id        string,
    user_id           string,
    product_count     int,
    price             decimal(10, 4),
    discount          decimal(10, 4),
    order_status      string,
    order_create_time timestamp,
    order_update_time timestamp,
    actual            decimal(10, 4),
    job_id            string,
    start_time        timestamp,
    end_time          timestamp,
    is_latest         string,
    is_active         string
) partitioned by (`year` string, `month` string, `day` string);

create table if not exists dim.t_dim_product
(
    product_id      string,
    mid             string,
    name            string,
    version         string,
    status          string,
    create_time     timestamp,
    update_time     timestamp,
    job_id          string,
    start_time      timestamp,
    end_time        timestamp,
    is_latest       string,
    is_active       string,
    is_auto_created string
) partitioned by (`year` string, `month` string, `day` string);


create table if not exists dim.t_dim_user
(
    dim_user_id     string,
    user_info_code  string,
    user_name       string,
    user_age        int,
    user_address    string,
    create_time     timestamp,
    update_time     timestamp,
    job_id          string,
    start_time      timestamp,
    end_time        timestamp,
    is_latest       string,
    is_active       string,
    is_auto_created string
) partitioned by (`year` string, `month` string, `day` string);
```

hive report:

```sql
-- ==report层 华为mate40-v2真实的销量表
create database if not exists report;
create table if not exists report.t_fact_order_report_actual(
	order_sn	varchar(128),
    product_id	varchar(128),
    product_code	varchar(128),
    product_name	varchar(128),
    product_version	varchar(128),
    product_status	varchar(128),
    price	decimal(10,4),
    discount	decimal(10,4),
    order_status	varchar(128),
    order_create_time	timestamp,
    order_update_time	timestamp,
    actual	decimal(10,4)
);

-- ==report层 华为mate40-v2算上v1的销量
create table if not exists report.t_fact_order_report_latest(
	order_sn	varchar(128),
    product_id	varchar(128),
    product_code	varchar(128),
    product_name	varchar(128),
    product_version	varchar(128),
    product_status	varchar(128),
    price	decimal(10,4),
    discount	decimal(10,4),
    order_status	varchar(128),
    order_create_time	timestamp,
    order_update_time	timestamp,
    actual	decimal(10,4)
);
```


### 运行从源到ods的任务

1. 准备数据

为dwd插入预先提供的数据

```sql
set hive.exec.dynamic.partition=true;
truncate table dwd.t_fact_order;
insert into dwd.t_fact_order partition (`year` = '2022', `month` = '04', `day` = '04') (order_id, order_sn, product_id,
                                                                                        user_id, product_count, price,
                                                                                        discount, order_status,
                                                                                        order_create_time,
                                                                                        order_update_time, actual,
                                                                                        start_time, end_time, is_active,
                                                                                        is_latest,
                                                                                        job_id)
values ('aaa', 'AAA', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 20, 0.3, 1,
        '2022-04-04 10:00:00', '2022-04-04 10:00:00', 19.7, '2022-04-04 10:00:00', null, '1', '1', 1),
       ('bbb', 'BBB', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 10, 0.3, 2,
        '2022-04-04 11:00:00', '2022-04-04 11:00:00', 9.7, '2022-04-04 11:00:00', null, '1', '1', 1),
       ('ccc', 'CCC', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 30, 0.3, 1,
        '2022-04-04 12:00:00', '2022-04-04 12:00:00', 29.7, '2022-04-04 12:00:00', null, '1', '1', 1);

truncate table dim.t_dim_user;
insert into dim.t_dim_user partition (year = '2022', month = '04', day = '04') (dim_user_id, user_info_code, user_name,
                                                                                user_age, user_address, create_time,
                                                                                update_time,
                                                                                start_time, end_time, is_active,
                                                                                is_latest, is_auto_created)
values ('06347be1-f752-4228-8480-4528a2166e14', 'u1', '张三', 12, '胜利街道', '2020-01-01 10:00:00', '2020-01-01 10:00:00',
        '2020-01-01 10:00:00', null, '1', '1', '0');

truncate table dim.t_dim_product;

insert into dim.t_dim_product partition (year = '2022', month = '04', day = '04') (product_id, mid, name, version,
                                                                                   status, create_time, update_time,
                                                                                   start_time, end_time,
                                                                                   is_active,
                                                                                   is_latest, is_auto_created)
values ('3abd0495-9abe-44a0-b95b-0e42aeadc807', 'p1', '华为', 'mate40', '上架', '2022-01-01 10:00:00',
        '2022-01-01 10:00:00', '2022-01-01 10:00:00', '2022-04-04 13:00:00', '0', '0', '0')
     , ('a9cd4e31-9268-4ee9-94b4-18c5e839a937', 'p1', '华为', 'mate40-v2', '上架', '2022-01-01 10:00:00',
        '2022-04-04 13:00:00', '2022-04-04 13:00:00', null, '1', '1', '0');

```

准备源表数据

```sql
truncate table sales.order;
insert into sales.order (order_sn, product_code, product_name, product_version, product_status,
                         user_code, user_name, user_age, user_address, product_count, price, discount, order_status,
                         order_create_time, order_update_time)
values ('AAA', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '胜利街道', 12, 20, 0.3, 2, '2022-04-04 10:00:00',
        '2022-04-08 10:00:00') -- 正常更新
     , ('DDD', 'p1', '华为', 'mate40-v2', '上架', 'u2', '李四', 32, '迎宾街道', 15, 200, 0.4, 2, '2022-04-08 09:00:00',
        '2022-04-08 10:00:00'); -- 新增
```


2. 下载已经编辑好的 [excel模板](https://docs.google.com/spreadsheets/d/1Zn_Q-QUTf6us4RwdgUgBosXL09-D-TowmgwWlDskvlA/edit?usp=sharing) 到 `~/Desktop` 来准备生成对应的任务脚本

3. 通过这个命令生成任务脚本

```bash
./gradlew :spark:run --args="generate-ods-sql -f ~/Desktop/ods.xlsx --output ~/Downloads/SharpETL/spark/src/main/resources/tasks"
```

4. 你看到如下日志表示任务脚本已经生成好了

```log
2022/08/02 17:23:51 INFO  [ETLLogger] - Write sql file to /Users/izhangzhihao/Downloads/SharpETL/spark/src/main/resources/tasks/ods__t_order.sql
2022/08/02 17:23:51 INFO  [ETLLogger] - Write sql file to /Users/izhangzhihao/Downloads/SharpETL/spark/src/main/resources/tasks/ods__t_user.sql
2022/08/02 17:23:51 INFO  [ETLLogger] - Write sql file to /Users/izhangzhihao/Downloads/SharpETL/spark/src/main/resources/tasks/ods__t_product.sql
```

5. 准备连接信息

```properties
sales.postgres.url=jdbc:postgresql://localhost:5432/postgres?stringtype=unspecified
sales.postgres.user=postgres
sales.postgres.password=postgres
sales.postgres.driver=org.postgresql.Driver
sales.postgres.fetchsize=10
```

6. 通过脚本启动任务

```bash
./gradlew :spark:run --args="batch-job --names=ods__t_order --period=1440 --default-start-time='2022-04-08 00:00:00' --once"
```

运行结果:

```log
Total jobs: 1, success: 1, failed: 0, skipped: 0
Details:
job name: ods__t_order SUCCESS x 1
```

### 运行从ods到dwd的任务

1. 下载提前准备好的[excel](https://docs.google.com/spreadsheets/d/1CetkqBsXj_E8oZBsws9iGdaJB1QJUajnwqH4FoKhXKA/edit?usp=sharing)到桌面

2. 通过这个命令生成任务脚本

```bash
./gradlew :spark:run --args="generate-dwd-sql -f ~/Desktop/dwd.xlsx --output ~/Downloads/SharpETL/spark/src/main/resources/tasks"
```

3. 你看到如下日志表示任务脚本已经生成好了

```log
2022/08/03 09:02:26 INFO  [ETLLogger] - Write sql file to /Users/izhangzhihao/Downloads/SharpETL/spark/src/main/resources/tasks/t_order_t_fact_order.sql
2022/08/03 09:02:26 INFO  [ETLLogger] - Write sql file to /Users/izhangzhihao/Downloads/SharpETL/spark/src/main/resources/tasks/t_user_t_dim_user.sql
2022/08/03 09:02:26 INFO  [ETLLogger] - Write sql file to /Users/izhangzhihao/Downloads/SharpETL/spark/src/main/resources/tasks/t_product_t_dim_product.sql
```

4. 通过脚本启动任务

```bash
./gradlew :spark:run --args="batch-job --names=t_order_t_fact_order --period=1440 --default-start-time='2022-04-08 00:00:00' --once"
```

运行结果:

```log
Total jobs: 1, success: 1, failed: 0, skipped: 0
Details:
job name: t_order_t_fact_order SUCCESS x 1
```

### 运行从dwd到report的任务

1. 手动创建两个step，分别代表两个report的需求, 华为mate40-v2真实的销量表,并将其放在`~/Downloads/sharp-etl/spark/src/main/resources/tasks/`路径下，命名为`order_report_actual_hive.sql`

```sql
-- workflow=report__t_fact_order_report_actual
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=hive
--  dbName=dwd
--  tableName=t_fact_order
-- target=hive
--  dbName=report
--  tableName=t_fact_order_report_actual
-- writeMode=overwrite
select fact.order_sn          as order_sn,
       dim.product_id         as product_id,
       dim.mid                as product_code,
       dim.name               as product_name,
       dim.version            as product_version,
       dim.status             as product_status,
       fact.price             as price,
       fact.discount          as discount,
       fact.order_status      as order_status,
       fact.order_create_time as order_create_time,
       fact.order_update_time as order_update_time,
       fact.actual            as actual
from dwd.t_fact_order fact
         inner join dim.t_dim_product dim
                    on fact.product_id = dim.product_id
                        and fact.is_latest = '1';
```

2. 手动创建两个step，分别代表两个report的需求, 华为mate40-v2算上v1的销量,并将其放在`~/Downloads/sharp-etl/spark/src/main/resources/tasks/`路径下，命名为`order_report_latest_hive.sql`

```sql
-- workflow=report__t_fact_order_report_latest
--  period=1440
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=hive
--  dbName=dwd
--  tableName=t_fact_order
-- target=hive
--  dbName=report
--  tableName=t_fact_order_report_latest
-- writeMode=overwrite
select fact.order_sn          as order_sn,
       dim2.product_id        as product_id,
       dim2.mid               as product_code,
       dim2.name              as product_name,
       dim2.version           as product_version,
       dim2.status            as product_status,
       fact.price             as price,
       fact.discount          as discount,
       fact.order_status      as order_status,
       fact.order_create_time as order_create_time,
       fact.order_update_time as order_update_time,
       fact.actual            as actual
from dwd.t_fact_order fact
         inner join dim.t_dim_product dim on fact.product_id = dim.product_id and fact.is_latest = '1'
         inner join (select * from dim.t_dim_product dim_latest where is_latest = '1') dim2
                    on dim.mid = dim2.mid and fact.is_latest = '1';
```

2. 通过脚本启动任务

```bash
./gradlew :spark:run --args="batch-job --names=order_report_actual_hive,order_report_latest_hive --period=1440 --default-start-time='2022-04-08 00:00:00' --once"
```
