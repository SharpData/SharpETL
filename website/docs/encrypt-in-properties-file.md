---
title: "Encrypt your confidential information in properties file"
sidebar_position: 8
toc: true
last_modified_at: 2021-12-23T18:25:57-04:00
---

## generate your private key `etl.key`

1. open the test `ETLConfigSpec`.
2. replace the path where you want to save the key.
3. replace the content which is the encryter password key.
4. if you want to use the offset, you can replace to 10 to the number that you want.
5. run the test.

## Set up it in your env

1. upload the key file to hdfs path. eg, `hdfs:///etl/conf/etl.key`
2. update the properties file in hdfs. eg, `encrpy.keyPath=hdfs:///etl/conf/etl.key`
3. if you have set a new offset instead of 10, you need to update the properties file in hdfs. eg, `encrypt.offset=11xx`

## using encrypt command

1. prepare `application.properties`:

```properties
encrypt.algorithm=PBEWithMD5AndDES
encrypt.keyPath=/path/to/etl.key
```

2. run encrypt command

```bash
./gradlew :spark:run --args="encrypt -p file:///path/to/application.properties 'content_to_be_encrypted'"
```
