---
title: "Docker setup"
sidebar_position: 5
toc: true
last_modified_at: 2021-11-03T18:25:57-04:00
---

This guide provides quick docker setup for local testing

## Requirments

- Docker
- Docker compose

## Setup step by step

```bash
cd docker
docker compose up -d # to start ETL database(mysql 5.7.28) & hive instance(version 3.1.2)
```

To access local hive instance you need

in `spark/build.gradle`

```diff
+ implementation "org.apache.spark:spark-hive_$scalaVersion:$sparkVersion"
```

add `hive-site.xml` in `spark/src/main/resources/hive-site.xml`

```xml
<configuration xmlns:xi="http://www.w3.org/2001/XInclude">
    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://localhost:9083</value>
    </property>

    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>file:///Users/$(whoami)/Documents/warehouse</value>
    </property>

    <property>
        <name>hive.metastore.warehouse.external.dir</name>
        <value>file:///Users/$(whoami)/Documents/warehouse</value>
    </property>
</configuration>
```

add `core-site.xml` in `spark/src/main/resources/core-site.xml`

```xml
<configuration xmlns:xi="http://www.w3.org/2001/XInclude">
    <property>
        <name>fs.defaultFS</name>
        <value>file:///Users/$(whoami)/Documents/warehouse</value>
        <final>true</final>
    </property>
</configuration>
```
