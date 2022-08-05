---
slug: sharp-etl-introduce-02-beyond-existing-etl
title: Sharp ETL介绍(二):超越现有ETL
tags: [sharp etl, data quality check]
date: 2022-08-02T00:00:00+08:00
---

## 导言


本文将从以下几个维度展开Sharp ETL的数据工程化实践：

* 通过step组合成为workflow
* 支持通过自定义代码逻辑扩展
* 工程化代码生成，固化统一且标准的数据工程实践
    * 事实表和维度表的关联检查
    * 记录表与表关联过程中的未知值和不适用值
    * 数据质量问题分级分类记录
	* ... ...

<!--truncate-->

## 通过workflow组织任务逻辑

在软件工程中，处理超长代码的方式可能是将代码逻辑为小而易于理解的单元，然后抽取方法，并给方法起通俗易懂的名字。这使得代码可重用并可以提高可读性。在处理超长SQL时经常会用到`WITH`语法：

```sql
WITH query_name1 AS (
     SELECT ...
     )
   , query_name2 AS (
     SELECT ...
       FROM query_name1
        ...
     )
SELECT ...
```

不可否认通过with语法重写之后可读性大大提升，但是我们认为这仍然不够。通过Sharp ETL的step拆分过后的SQL可读性更好，且debug更为容易。
乍一看似乎通过workflow组织的SQL更加长或者复杂，实则不然，每一个step都可以有名称来解释这个step的作用，其实通过with语句组织的SQL也经常需要注释来解释。同时这里还有一个隐藏点：如果source是temp，就可以不用写source，一定程度上能够简化理解。同时因为日志驱动的存在，每一个step在执行时都记录了source和target的条数，这个相对于直接写WITH语法更容易调试和排错。


```sql
-- step=define query name 1
-- source=source type xxx
-- target=temp
--  tableName=query_name1
SELECT ...

-- step=define query name 2
-- target=temp
--  tableName=query_name2
SELECT ...
       FROM query_name1

-- step=output
-- target=target type xxx
--  tableName=target table
SELECT ...

```

## workflow的未来

实际上最初版本的workflow是顺序执行的，一个step接着一个step。但在真正使用时我们往往需要例如 分支判断、循环、抛出异常、错误处理分支 等等功能，这些都在Sharp ETL未来的计划中，未来会逐步增加这些功能。

## 通过[`Transformer`](https://github.com/SharpData/SharpETL/blob/97f303cbd1f40a29780551851f690c283bcb2061/spark/src/main/scala/com/github/sharpdata/sharpetl/spark/transformation/Transformer.scala)来进行自定义逻辑扩展

我们可以先看一下`Transformer`的API，它提供了在读写数据时插入自定义逻辑的机制，用户可以轻易的通过实现这个API来插入自己的逻辑。甚至可以通过动态加载scala脚本文件来扩展而不需要重新build jar包，可以参考[这里](/docs/transformer-guide)。

```scala
trait Transformer {

  /**
   * read
   */
  def transform(args: Map[String, String]): DataFrame = ???

  /**
   * write
   */
  def transform(df: DataFrame, step: WorkflowStep, variables: Variables): Unit = ???
}
```

## 工程化代码生成


如果是完全手写SQL来完成所有功能，包括数据工程功能，整个逻辑会非常长和复杂。为了降低使用门槛，使得整个过程更加轻松，我们需要通过一些手段将复杂度封装起来。考虑到相同场景下不同表的ETL实现可能是十分类似的，我们想到了通过定义模板来进行数据建模，通过模板来生成workflow这样的方式。你在 [quick start](/docs/quick-start-guide#generate-sql-files-from-excel-config) 里面肯定也见到了通过填写excel模板来生成workflow的方式。实际上我们认为除了具有非常特殊逻辑的任务可以手写以外，绝大多数任务都应该通过模板定义，然后生成。这样有几个好处：

* 避免了手写任务可能造成的数据工程化实践不一致甚至出错
* 当大量任务需要修改时，只需要修改模板内容，并重新生成即可
* excel模板作为大家都能理解的中间产物，可以作为数据BA和数据工程师DE的沟通桥梁，降低团队间的沟通和摩擦成本
* 团队可以不需重复造轮子，在一个新的项目可以快速进入业务开发，减少大量的基础设置投入


### 数据质量问题分级分类

“事实表、维度表的关联检查”、“记录表与表关联过程中的未知值和不适用值” 前一篇文章已经提过了，这里重点展开下 “数据质量问题分级分类”

首先看一下我们的数据质量检查配置文件，我们允许通过类SQL或者自定义代码(User Defined Rule)的方式来定义数据质量规则。这其中可以使用 `$column` 来引用被应用规则的column name，可以使用UDF，也可以加载一段代码来实现数据质量检查的规则。

```yaml
- dataCheckType: power null check
  rule: powerNullCheck($column)
  errorType: error
- dataCheckType: null check
  rule: $column IS NULL
  errorType: error
- dataCheckType: duplicated check
  rule: UDR.com.github.sharpdata.sharpetl.core.quality.udr.DuplicatedCheck
  errorType: warn
- dataCheckType: mismatch dim check
  rule: $column = '-1'
  errorType: warn
```

配置文件中定义的数据质量规则是全局共享的，需要使用数据质量检查的地方可以通过column.column name.qualityCheckRules=rule name引用已有的规则：

```sql
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

select ....
```

数据质量规则同时支持定义error和warn级别的错误，通常warn级别的错误是可以接受的，而error级别的错误是不被认为可接受的，往往不会进入数据仓库。但是error和warn级别的错误都会分级分类记录下来，方便后续排错或者发送通知来解决问题。


*下一篇文章将具体介绍日志驱动的逻辑和实现。*