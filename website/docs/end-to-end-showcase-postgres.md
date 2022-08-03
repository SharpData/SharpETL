---
title: "End to end showcase(Postgres)"
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

drop table if exists sales.order;
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

drop table if exists sales.user;
create table if not exists sales.user
(
    user_code    varchar(128),
    user_name    varchar(128),
    user_age     int,
    user_address varchar(128),
    create_time  timestamp,
    update_time  timestamp
);

drop table if exists sales.product;
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


Postgres ods:

```sql
create schema if not exists ods;

drop table if exists ods.t_order;
create table if not exists ods.t_order
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
    order_update_time timestamp,
    job_id            varchar(128)
) ;

drop table if exists ods.t_user;
create table if not exists ods.t_user
(
    user_code    varchar(128),
    user_name    varchar(128),
    user_age     int,
    user_address varchar(128),
    create_time  timestamp,
    update_time  timestamp,
    job_id       varchar(128)
);

drop table if exists ods.t_product;
create table if not exists ods.t_product
(
    product_code    varchar(128),
    product_name    varchar(128),
    product_version varchar(128),
    product_status  varchar(128),
    create_time     timestamp,
    update_time     timestamp,
    job_id          varchar(128)
);
```

Postgres dwd:

```sql
SET search_path TO dwd, public;
create extension if not exists "uuid-ossp";
create schema if not exists dwd;
drop table if exists dwd.t_fact_order;
create table dwd.t_fact_order
(
    order_sn          varchar(128),
    product_id        varchar(128),
    user_id           varchar(128),
    product_count     int,
    price             decimal(10, 4),
    discount          decimal(10, 4),
    order_status      varchar(128),
    order_create_time timestamp,
    order_update_time timestamp,
    actual            decimal(10, 4),
    job_id            varchar(128)
);

drop table if exists dwd.t_dim_product;
create table if not exists dwd.t_dim_product
(
    product_id      varchar(128) default uuid_generate_v1(),
    mid             varchar(128),
    name            varchar(128),
    version         varchar(128),
    status          varchar(128),
    create_time     timestamp,
    update_time     timestamp,
    job_id          varchar(128),
    start_time      timestamp,
    end_time        timestamp,
    is_latest       varchar(1),
    is_active       varchar(1),
    is_auto_created varchar(1)
);

drop table if exists dwd.t_dim_user;
create table if not exists dwd.t_dim_user
(
    dim_user_id     varchar(128) default uuid_generate_v1(),
    user_info_code  varchar(128),
    user_name       varchar(128),
    user_age        int,
    user_address    varchar(128),
    create_time     timestamp,
    update_time     timestamp,
    job_id          varchar(128),
    start_time      timestamp,
    end_time        timestamp,
    is_latest       varchar(1),
    is_active       varchar(1),
    is_auto_created varchar(1)
);
```

Postgres report:

```sql
-- ==report层 华为mate40-v2真实的销量表
create schema if not exists report;
drop table if exists report.t_fact_order_report_actual;
create table report.t_fact_order_report_actual(
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
create schema if not exists report;
drop table if exists report.t_fact_order_report_latest;
create table report.t_fact_order_report_latest(
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

0. 准备数据

为dwd插入预先提供的数据

```sql
truncate table dwd.t_fact_order;
insert into dwd.t_fact_order (order_sn, product_id,
                              user_id, product_count, price,
                              discount, order_status,
                              order_create_time,
                              order_update_time, actual,
                              job_id)
values ('AAA', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 20, 0.3, 1,
        '2022-04-04 10:00:00', '2022-04-04 10:00:00', 19.7, 1),
       ('BBB', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 10, 0.3, 2,
        '2022-04-04 11:00:00', '2022-04-08 11:00:00', 9.7, 1),
       ('CCC', '3abd0495-9abe-44a0-b95b-0e42aeadc807', '06347be1-f752-4228-8480-4528a2166e14', 12, 30, 0.3, 1,
        '2022-04-04 12:00:00', '2022-04-04 12:00:00', 29.7, 1);

truncate table dwd.t_dim_user;
insert into dwd.t_dim_user (dim_user_id, user_info_code, user_name,
                            user_age, user_address, create_time,
                            update_time,
                            start_time, end_time, is_active,
                            is_latest, is_auto_created)
values ('06347be1-f752-4228-8480-4528a2166e14', 'u1', '张三', 12, '胜利街道', '2020-01-01 10:00:00', '2020-01-01 10:00:00',
        '2020-01-01 10:00:00', null, '1', '1', '0');


truncate table dwd.t_dim_product;
insert into dwd.t_dim_product (product_id, mid, name, version,
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
     , ('AAA', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '胜利街道', 12, 20, 0.3, 2, '2022-04-04 10:00:00',
        '2022-04-08 10:00:00') -- 重复数据
     , ('BBB', 'p1', '华为', 'mate40', '上架', 'u1', '张三', 12, '胜利街道', 12, 10, 0.3, 1, '2022-04-04 10:00:00',
        '2022-04-08 10:00:00') -- 时间顺序错乱，不做修改
     , ('DDD', 'p1', '华为', 'mate40-v2', '上架', 'u2', '李四', 32, '迎宾街道', 15, 200, 0.4, 1, '2022-04-08 09:00:00',
        '2022-04-08 10:00:00') -- 新增，华为p1名称修改
     , ('DDD', 'p2', '华为', 'mate50', '上架', 'u3', '李四', 32, '迎宾街道', 15, 330, 0.4, 1, '2022-04-08 09:00:00',
        '2022-04-08 10:00:00'); -- 新增， p2迟到维不作处理的场景
```


1. 下载已经编辑好的 [soudce->ods模板](https://docs.google.com/spreadsheets/d/1vvWq26t7i_9bFXaRMQpQsFKXnBBHkIEzVEI3EI_eLIg/edit#gid=0) 到 `~/Desktop` 来准备生成对应的任务脚本

2. 通过这个命令生成任务脚本

```bash
./gradlew :spark:run --args="generate-ods-sql -f ~/Desktop/postgres-ods.xlsx --output ~/Downloads/SharpETL/spark/src/main/resources/tasks/"
```

3. 你看到如下日志表示任务脚本已经生成好了

```log
2022/08/03 10:54:49 INFO  [ETLLogger] - Write sql file to /Users/xiaoqiangma/Downloads/SharpETL/spark/src/main/resources/tasks/ods__ods.t_order.sql
2022/08/03 10:54:49 INFO  [ETLLogger] - Write sql file to /Users/xiaoqiangma/Downloads/SharpETL/spark/src/main/resources/tasks/ods__ods.t_user.sql
2022/08/03 10:54:49 INFO  [ETLLogger] - Write sql file to /Users/xiaoqiangma/Downloads/SharpETL/spark/src/main/resources/tasks/ods__ods.t_product.sql
```

4. 创建ods表


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
./gradlew :spark:run --args="single-job --name=ods__ods.t_order --period=1440 --default-start-time='2022-04-08 00:00:00' --local --once"
```



### 运行从ods到dwd的任务

1. 下载提前准备好的[ods->dwd 模板](https://docs.google.com/spreadsheets/d/19pOIogg31JWRUiKYyFLnQXCnPihYFOitamlfzrIXWfE/edit#gid=0)到桌面

2. 通过这个命令生成任务脚本

```bash
./gradlew :spark:run --args="generate-dwd-sql -f ~/Desktop/postgres-dwd.xlsx --output ~/Downloads/SharpETL/spark/src/main/resources/tasks/"
```

3. 你看到如下日志表示任务脚本已经生成好了

```log
2022/08/03 10:58:10 INFO  [ETLLogger] - Write sql file to /Users/xiaoqiangma/Downloads/SharpETL/spark/src/main/resources/tasks/ods.t_order_dwd.t_fact_order.sql
```

4. 创建dwd/dim表

6. 通过脚本启动任务

```bash
./gradlew :spark:run --args="single-job --name=ods.t_order_dwd.t_fact_order --period=1440 --local"
```



### 运行从dwd到report的任务

1. 手动创建两个step，分别代表两个report的需求：
    1. report层 华为mate40-v2真实的销量表,并将其放在`~/Downloads/SharpETL/spark/src/main/resources/tasks/`路径下，命名为`order_report_actual.sql`

       ```sql
       -- workflow=order_report_actual
       --  period=1440
       --  loadType=incremental
       --  logDrivenType=timewindow
       
       -- step=1
       -- sourceConfig
       --  dataSourceType=postgres
       --  dbName=postgres
       --  tableName=dwd.t_fact_order
       -- targetConfig
       --  dataSourceType=postgres
       --  dbName=postgres
       --  tableName=report.t_fact_order_report_actual
       -- writeMode=overwrite
       -- incrementalType=depend_on_upstream
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
       ```

    2. report层 华为mate40-v2算上v1的销量,并将其放在`~/Downloads/SharpETL/spark/src/main/resources/tasks/`路径下，命名为`order_report_latest.sql`

       ```sql
       -- workflow=order_report_latest
       --  period=1440
       --  loadType=incremental
       --  logDrivenType=timewindow
       
       -- step=1
       -- sourceConfig
       --  dataSourceType=postgres
       --  dbName=postgres
       --  tableName=dwd.t_fact_order
       -- targetConfig
       --  dataSourceType=postgres
       --  dbName=postgres
       --  tableName=report.t_fact_order_report_latest
       -- writeMode=overwrite
       -- incrementalType=depend_on_upstream
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
       ```

2. 通过脚本启动任务

```bash
./gradlew :spark:run --args="single-job --name=order_report_actual --period=1440 --local"
```
```bash
./gradlew :spark:run --args="single-job --name=order_report_latest --period=1440 --local"
```


