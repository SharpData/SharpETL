---
slug: sharp-etl-introduce-01-why-we-need-sharp-etl
title: Sharp ETL介绍(一):为什么我们需要Sharp ETL
tags: [sharp etl]
date: 2022-08-01T00:00:00+08:00
---

## 导言

本文结合目前的数据工程实践，尝试展开数据工程实践中ETL的原则，并对Sharp ETL做简要介绍。

ETL或ELT是进行数据处理的常见手段之一，随着数据平台渡过蛮荒时代开始精细化治理，原始的编码方式和无组织的SQL脚本已经不能满足需求。我们将会通过展开现有ETL在当前遇到的困境来引入Sharp ETL的独有但必不可缺的功能。

<!--truncate-->

## 之前的ETL有什么问题

常见的ETL形式有几种，这几种方式各有优劣，我们分别从几个维度展开一下：

1. **代码方式**

代码方式是非常常见的ETL编写方式，广泛适用于各种场景。

pros：

* 代码编写的ETL更容易随意扩展、增加自定义逻辑和复用代码逻辑。

cons：

* 但是代码就是代码，大多数代码都是命令式逻辑，并不容易维护和理解。
* 缺乏足够的元数据支撑，不知道这个任务访问了哪几张表。当要修改表结构的时候，可能导致意料之外的任务失败。
* 代码形式编写ETL会使得任务开发流程更加重量级，因为一旦修改实现逻辑，就需要重新部署jar包。考虑到安全因素，在一些企业内部的多租户平台上，上传新的jar包是需要审核的，整个流程就会在审核这里显著慢了下来。即使不需要审核，也需要手动部署到环境上，也有可能因为增加了新的逻辑影响了其他正常运行的任务。

2. **托拉拽**

托拉拽在新兴云平台和企业自建平台上非常常见，几乎是所有平台的标配。

pros：

* DAG形式也很容易理解，开发门槛低，只需要填入参数，轻松实现数据接入、数据去重等等逻辑。
* 增加、修改逻辑都相当轻量级，甚至大多数平台也实现了版本管理，可以像管理代码一样管理任务，可以轻松做到回退版本、解决冲突等类似git的操作。
* 部分平台可以通过小的组件组织成为更大的组件来复用ETL的逻辑。

cons：

* 自定义逻辑较困难，甚至在一些封闭性较高的平台上增加新的数据源都是问题。
* 界面操作难以自动化，当需要批量针对若干任务进行某项操作时，图形化界面就不方便了。有时我们会遇到某个业务系统内需要的ETL的逻辑都很类似，但是任务数量很多，手动托拉拽很费事也容易出错。

3. **纯SQL脚本**

pros：

* 经过合适拆分的SQL逻辑更容易理解（单条SQL的行数需要有限制），声明式实现更加表意，基本上人人都会SQL，门槛很低。

cons：

* 逻辑难以复用，经常需要复制另外一段SQL的逻辑。
* 缺乏系统性编排，纯SQL脚本往往比较散乱。
* 扩展受限，虽然各种SQL方言基本上都是支持UDF（User Defined Function）的，但是扩展能力仍然比不上代码（比如需要和某个HTTP API交互）。

### 数据工程实践缺失

我们认为，在ETL开发中需要包括这些工程实践：

* 通过事实表和维度表的关联来检查数据一致性、完整性
* 通过特殊值来记录表与表关联过程中的未知值(关联不上)和不适用值(不合理值)
* 根据基于业务定义的数据质量检查规则在日志中分级分类记录数据质量问题，确保数据的完整性和准确性
* 通过记录调度日志来与特定调度服务解耦合，增强调度的鲁棒性，同时也能够结构化的记录任务运行过程中的信息，方便排错
* 可以及时主动识别上游系统的变更（主要是表结构等），并及时做出相应调整
* 显式强调增量/全量、渐变/非渐变，作为任务的元数据
* 任务依赖检查，只有当任务依赖满足时才执行任务

而无论是代码编写、托拉拽还是纯SQL脚本都因为过多人工操作使得这些数据工程实践难以统一，常常有以下问题：

* 只做关联而并不记录数据一致性、完整性问题。甚至缺乏最基本的关联。
* 由于时间紧张、实现复杂等因素，数据质量检查操作缺失，导致进入数仓的数据缺乏最基本的质量保证。
* 数据运维难以自动化，任务失败后多需要人工介入后才可以使任务恢复正常。
* 任务调度过度依赖于已有调度服务，缺乏防呆设计，难以解决重复调度的问题（假如调度服务出现异常，任务被重复调度了，结果就是重复计算）
* 错误排查过度依赖于任务执行日志，排查错误多依赖于不停的推断、加日志和尝试。如果是纯SQL脚本，则经常遇到几千行的SQL任务失败，然后不知道从哪里开始debug的窘境。

这些工程实践往往会被人忽略而导致后续的数据问题，所以我们需要一个为我们封装好了上述工程实践的框架，从而确保ETL的高质量和低运维。

## 理想中的ETL应该什么样子？

近些年来最火热的莫过于流处理、流式ETL这些概念。虽然这篇文章与流或者批处理没有直接关系，还是要指出，无论是流式处理还是批处理，最基本的数据工程实践都需要具备，这些数据工程实践都是实际实践中不可或缺的一部分。以下讨论且抛开流式或批处理不谈。

理想中的ETL应该能结合以上集中实现方式的优点，摒弃缺点，我们可以来构思一下：

* 拥有SQL的语义化表达能力
* 支持通过自定义代码逻辑扩展
* 支持代码优先、版本管理
* 在代码优先的基础上能够支持可视化工具构建
* 能够对过长的SQL进行拆解，降低理解难度
* 内置统一且标准的数据工程实践
    * 事实表和维度表的关联检查
    * 记录表与表关联过程中的未知值和不适用值
    * 数据质量问题分级分类记录
    * 通过调度日志解耦合调度系统
	* ... ...


### Sharp ETL workflow示意

下面示意一下Sharp ETL的一个workflow，我会分别对于这些step做出解释。

workflow一开始是一个header，描述workflow本身的属性，包括workflow名称、增量还是全量、日志驱动方式、通知方式等等。然后是一系列step，每个step都有自己的输入输出。

第一个step表示从hive中取数据出来写到temp表以便后续使用，这个SQL中也用到了Sharp ETL内置的变量功能，`${YEAR}` `${MONTH}` `${DAY}` 这些变量都会在运行时被替换为真实的值，真实值通过我们内置的调度日志来计算得出。这里请注意在source的options中有配置数据质量检查，这里只需要引用不同的数据质量检查规则，即可对相应字段进行数据质量检查，并根据规则定义的级别进行分级分类管理

第二个step是进行事实表和维度表的关联，并通过`-1`来记录未关联上的数据。

第三个step是对temp的数据做预处理。

第四个step是对hive数据更新的特殊处理，在hive上如果要更新数据就需要把数据对应的分区全部更新掉，所以这里是在计算本次数据涉及到更新哪些分区，以方便后续将这些分区的数据取出来做更新。

第五个step用到了上一个step拼接的查询条件参数，将此次更新设计到的数据分区都取出来。

第六个step是通过Transformer功能调用一段代码来实现拉链表，其实这段代码也是通过拼接SQL实现的，只不过拼接过程较为灵活，才以一个transformer来封装。实际上transformer中可以执行任意代码。transformer本身也有输出，这里我们将输出覆盖到对应的分区，完成任务。


```sql
-- workflow=fact_device
--  loadType=incremental
--  logDrivenType=timewindow

-- step=1
-- source=hive
--  dbName=ods
--  tableName=t_device
--  options
--   idColumn=order_id
--   column.device_id.qualityCheckRules=power null check
--   column.status.qualityCheckRules=empty check
-- target=temp
--  tableName=t_device__extracted
-- writeMode=overwrite
select
	`device_id` as `device_id`,
	`manufacturer` as `manufacturer`,
	`status` as `status`,
	`online` as `online`,
	`create_time` as `create_time`,
	`update_time` as `update_time`,
	`year` as `year`,
	`month` as `month`,
	`day` as `day`
from `ods`.`t_device`
where `year` = '${YEAR}'
  and `month` = '${MONTH}'
  and `day` = '${DAY}';

-- step=2
-- source=temp
--  tableName=t_device__extracted
-- target=temp
--  tableName=t_device__joined
-- writeMode=append
select
	`t_device__extracted`.*,
	case when `t_dim_user`.`user_code` is null then '-1'
		else `t_dim_user`.`user_code`
	end as `user_id`
from `t_device__extracted`
left join `dim`.`t_dim_user` `t_dim_user`
 on `t_device__extracted`.`user_id` = `t_dim_user`.`user_code`
 and `t_device__extracted`.`create_time` >= `t_dim_user`.`start_time`
 and (`t_device__extracted`.`create_time` < `t_dim_user`.`end_time`
      or `t_dim_user`.`end_time` is null);

-- step=3
-- source=temp
--  tableName=t_device__joined
-- target=temp
--  tableName=t_device__target_selected
-- writeMode=overwrite
select
	`device_id`,
	`manufacturer`,
	`user_id`,
	`status`,
	`online`,
	`create_time`,
	`update_time`,
	`year`,
	`month`,
	`day`
from `t_device__joined`;

-- step=4
-- source=hive
--  dbName=dwd
--  tableName=t_fact_device
-- target=variables
select concat('where (',
  ifEmpty(
    concat_ws(')\n   or (', collect_set(concat_ws(' and ', concat('`year` = ', `year`), concat('`month` = ', `month`), concat('`day` = ', `day`)))),
    '1 = 1'),
  ')') as `DWD_UPDATED_PARTITION`
from (
  select
  dwd.*
  from `dwd`.`t_fact_device` dwd
  left join `t_device__target_selected` incremental_data on dwd.device_id = incremental_data.device_id
  where incremental_data.device_id is not null
        and dwd.is_latest = 1
);

-- step=5
-- source=hive
--  dbName=dwd
--  tableName=t_fact_device
-- target=temp
--  tableName=t_fact_device__changed_partition_view
select *
from `dwd`.`t_fact_device`
${DWD_UPDATED_PARTITION};

-- step=6
-- source=transformation
--  className=com.github.sharpdata.sharpetl.spark.transformation.SCDTransformer
--  methodName=transform
--   createTimeField=create_time
--   dwUpdateType=incremental
--   dwViewName=t_fact_device__changed_partition_view
--   odsViewName=t_device__target_selected
--   partitionField=create_time
--   partitionFormat=year/month/day
--   primaryFields=device_id
--   surrogateField=
--   timeFormat=yyyy-MM-dd HH:mm:ss
--   updateTimeField=update_time
--  transformerType=object
-- target=hive
--  dbName=dwd
--  tableName=t_fact_device
-- writeMode=overwrite
```

### One more thing!

其实上一段SQL是通过代码生成的，并非人工手写！在实际操作中，只需要填写我们的excel [ods模板](https://docs.google.com/spreadsheets/d/1Zn_Q-QUTf6us4RwdgUgBosXL09-D-TowmgwWlDskvlA) 或 [dwd模板](https://docs.google.com/spreadsheets/d/1CetkqBsXj_E8oZBsws9iGdaJB1QJUajnwqH4FoKhXKA)即可生成代码，可以成倍的提高开发效率。


*后续文章还将对内置的数据工程实践（包括但不限于“日志驱动”、“数据质量分级分类”等）和 Sharp ETL的实现等等进行介绍*


