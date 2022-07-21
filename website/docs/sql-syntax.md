---
title: "SQL syntax"
sidebar_position: 9
toc: true
last_modified_at: 2022-06-14T09:25:57+08:00
---


## About

Sharp-ETL is not a new sql language, but re-using existing,familiar SQL(like PostgresSQL, MSSQL, and any other sql you want!).


## Hello World

Let's start by creating a very simple workflow to do the "hello world" using Spark temp table:

```sql
-- workflow=hello_world
--  loadType=incremental
--  logDrivenType=timewindow

-- step=print SUCCESS to console
-- source=temp
-- target=console

SELECT 'SUCCESS' AS `RESULT`;
```

## Parameters

Let's look at a slightly more complex workflow spec with parameters.

