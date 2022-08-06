---
title: "Datasource"
sidebar_position: 1
toc: true
last_modified_at: 2022-08-05T18:25:57-04:00
---

## 支持的数据源

## Read
### hive

  该 step 直接通过 SparkSession （需启用 hive 支持）执行 hive sql 并返回 DataFrame 以供后续操作。

  参数：

  | 参数名称       | 默认值 | 是否可空 | 说明           |
    | -------------- | ------ | -------- | -------------- |
  | dataSourceType | 无     | 否       | 可选值：hive。 |

  示例：

  ```sql
  -- step=1
  -- source=hive
  -- target=console
  select *
  from table1;
  ```

### temp

  该 step 直接通过 SparkSession （不需启用 hive 支持）执行 hive sql 并返回 DataFrame 以供后续操作。

  参数：

  | 参数名称       | 默认值 | 是否可空 | 说明           |
    | -------------- | ------ | -------- | -------------- |
  | dataSourceType | 无     | 否       | 可选值：temp。 |

  示例：

  ```sql
  -- step=1
  -- source=temp
  -- target=temp
  --  tableName=temp_table
  select 1 as a;
  
  -- step=2
  -- source=temp
  -- target=console
  select *
  from temp_table;
  ```

### jdbc

    - mysql
    - oracle
    - postgres
    - ms_sql_server
    - impala
    - informix


  jdbc 类型的操作支持的配置项基本一致，区别只是数据源不同。

  jdbc 类操作需在 application.properties 中配置基本连接信息，格式为 `${dbName}.${dataSourceType}.*`，示例：

  ```properties
  test.postgres.driver=org.postgresql.Driver
  test.postgres.fetchsize=10
  test.postgres.url=jdbc:postgresql://localhost:5432/default?currentSchema="default"
  test.postgres.user=root
  test.postgres.password=root
  ```

  参数：

  | 参数名称        | 默认值 | 是否可空 | 说明                                                         |
    | --------------- | ------ | -------- | ------------------------------------------------------------ |
  | dataSourceType  | 无     | 否       | 可选值：mysql、oracle、postgres、ms_sql_server、impala。     |
  | dbName          | 无     | 是       | 如果指定了 dbName，会使用 application.properties 配置文件中以指定的 dbName 开头的配置项去读数据，不指定默认无前缀。 |
  | numPartitions   | 无     | 是       | 需要将一个大的查询拆分为多少个可并行的小查询。               |
  | partitionColumn | 无     | 是       | 拆分查询依据的字段。                                         |
  | lowerBound      | 无     | 是       | 拆分查询依据字段的最小值（spark2.3仅支持数字类型）。         |
  | upperBound      | 无     | 是       | 拆分查询依据字段的最大值（spark2.3仅支持数字类型）。         |

  示例：

  ```sql
  -- step=1
  -- source=postgres
  --  dbName=test
  -- target=variables
  select cast(min(id) as int8) as lowerBound,
         cast(max(id) as int8) as upperBound
  from test_table;
  
  -- step=2
  -- source=postgres
  --  dbName=test
  --  numPartitions=10000
  --  lowerBound=${lowerBound}
  --  upperBound=${upperBound}
  --  partitionColumn=id
  -- target=hive
  --  tableName=ods_test_table
  -- writeMode=overwrite
  select *
  from test_table;
  ```

### kudu

  该 step 通过 kudu spark 去读取 kudu 表中的数据，之后根据 selectSql 对 load 出来的数据做其他操作（基本的查询谓词可下推到 kudu）。

  kudu 表的读取需在 application.properties 中配置 `kudu.master`，示例：

  ```properties
  kudu.master=localhost:7051
  ```

  参数：

  | 参数名称       | 默认值 | 是否可空 | 说明           |
    | -------------- | ------ | -------- | -------------- |
  | dataSourceType | 无     | 否       | 可选值：kudu。 |
  | dbName         | 无     | 否       | kudu库名。     |
  | tableName      | 无     | 否       | kudu表名。     |

  示例：

  ```sql
  -- step=1
  -- source=kudu
  --  dbName=test_db
  --  tableName=test_table
  -- target=console
  select *
  from test_table;
  ```

### impala_kudu

  impale_kudu 与 kudu 查询的逻辑基本一致，都是通过 kudu spark 去操作数据，但不同的是 impale_kudu 是操作由 impala 托管的 kudu 表。由于 impala 默认会在建表时给表名加一个前缀，因此我们在 impala 中操作 kudu 表时使用的表名可能是 'test_db.test_table'，但通过 kudu 直接去操作时表名可能就是 `impala::test_db.test_table` 了。

  因此，需要在 application.properties 配置文件中额外配置 `kudu.table.prefix` 参数。

  ```properties
  kudu.master=localhost:7051
  kudu.table.prefix=impala::
  ```

  参数：

  | 参数名称       | 默认值 | 是否可空 | 说明                  |
    | -------------- | ------ | -------- | --------------------- |
  | dataSourceType | 无     | 否       | 可选值：impala_kudu。 |
  | dbName         | 无     | 否       | kudu库名。            |
  | tableName      | 无     | 否       | kudu表名。            |

  示例：

  ```sql
  -- step=1
  -- source=impala_kudu
  --  dbName=test_db
  --  tableName=test_table
  -- target=console
  select *
  from test_table;
  ```

### 文件传输类

  该 step 只是 copy 文件，不解析文件内容（不涉及 spark 操作）。暂时只支持 copy 文件到 hdfs。

  文件传输类操作配置内容基本一致，区别只是数据源不同。

  文件传输类操作需要在 application.properties 中配置服务器连接信息，格式为 `${configPrefix}.${dataSourceType}.*`，示例：

  ```properties
  test.ftp.host=localhost
  test.ftp.port=21
  test.ftp.user=root
  test.ftp.password=root
  test.ftp.dir=/test
  test.ftp.localTempDir=/tmp/ftp/test
  test.ftp.hdfsTempDir=/tmp/ftp/test
  
  test.scp.host=localhost
  test.scp.port=22
  test.scp.user=root
  test.scp.password=root
  test.scp.dir=/test
  test.scp.localTempDir=/tmp/scp/test
  test.scp.hdfsTempDir=/tmp/scp/test

  test.sftp.host=localhost
  test.sftp.port=22
  test.sftp.user=root
  test.sftp.password=root
  test.scp.proxy.host=localhost
  test.scp.proxy.port=22
  ```

  参数：

  | 参数名称        | 默认值 | 是否可空 | 说明                                                         |
    | --------------- | ------ | -------- | ------------------------------------------------------------ |
  | dataSourceType  | 无     | 否       | 可选值：scp、ftp、hdfs、sftp、mount。                                     |
  | configPrefix    | 无     | 否       | 需读取文件的系统连接配置前缀。                               |
  | fileDir         | 无     | 事       | 如果在 application.properties 中已配置过 `${configPrefix}.${dataSourceType}.dir` 参数，则此处无需再次指定 `fileDir`，如果重复指定，会已 `fileDir` 指定的目录为准。 |
  | fileNamePattern | .*     | 否       | 文件匹配正则。分三段，格式为："(前缀)(文件名主体)(后缀)"。<br />一般情况下前后缀都给个空括号即可。<br />个别情况下读取文件时需要根据同目录下的标志文件判断文件是否已写出完整。例如：正则为：`()(a\.txt)(\.OK)`，则目录下只存在 `a.txt` 文件时不会读取此文件，只有目录下同时存在 `a.txt.OK` 文件时才会去读取文件 `a.txt`。 |
  | deleteSource    | false  | 否       | 是否在文件复制完成后删除原始文件，默认false，不删除。        |
  | decompress      | false  | 否       | 是否需要解压后上传到 hdfs，默认 fasle，不解压。              |
  | codecExtension  | 无     | 是       | 需解压后上传到 hdfs 时，指定压缩格式后缀，例如：gz、zip。默认为空，不解压。 |
  | onlyOneName     | false  | 否       | 文件名是否始终不变。<br />如果文件名始终不变，则每次执行此 step 都会重复读取同一文件，反之则在第一次成功读取后就不会再次读取该文件了。 |

    - scp

      示例：

      ```sql
      -- step=1
      -- source=scp
      --  configPrefix=test
      --  fileNamePattern=()(a\.test\..+)(OK)
      --  deleteSource=false
      -- target=hdfs
      --  configPrefix=test
      -- writeMode=overwrite
      ```

    - ftp

      示例：

      ```sql
      -- step=1
      -- source=ftp
      --  configPrefix=test
      --  fileNamePattern=()(a\.test\..+)(OK)
      --  deleteSource=false
      -- target=hdfs
      --  configPrefix=test
      -- writeMode=overwrite
      ```

    - hdfs

      示例：

      ```sql
      -- step=1
      -- source=hdfs
      --  configPrefix=test
      --  fileNamePattern=()(a\.test.+)()
      -- target=ftp
      --  configPrefix=test
      --  filePath=/test
      -- writeMode=overwrite
      ```

    - sftp

      对于sftp,会将匹配的文件先下载到本地，然后再将本地文件上传到hdfs
    
      参数

      | 参数名称        | 默认值 | 是否可空 | 说明                                                         |
      | --------------- | ------ | -------- | ------------------------------------------------------------ |
      | sourceDir  | 无     | 否       | 表示sftp服务器里的绝对路径                                 |
      | tempDestination    | 无     | 否       | sftp文件存在的本地的临时路径                               |
      | tempDestinationDirPermission  | rw-rw----     | 是       | 表示临时路径的文件权限 |
      | hdfsDir | 无     | 否       |  | 上到到hdfs的路径 |
      | filterByTime | false | 是 | 可填true/false, 过滤sftp上的文件时，是否按文件修改时间过滤，如果是true,则会除了文件名正则匹配，也需要文件修改时间在[dataRangeStart, dataRangeEnd)之间 |
      | timezone | GMT+8 | 是 |    ETL Frames所在服务器的时区          |

      示例：

      ```sql
      -- step=1
      -- source=sftp
      --  configPrefix=test
      --  fileNamePattern=()(a\.test.+)()
      --  sourceDir=/Distribution/NMBOData
      --  tempDestinationDir=/data1/ticketflap/tmp
      --  hdfsDir=hdfs:///data/ticketflap/
      --  filterByTime=false
      -- target=ftp
      --  configPrefix=test
      --  filePath=/test
      -- writeMode=overwrite
      ```
    
    - mount(sharefolder)
    
      参数

      | 参数名称        | 默认值 | 是否可空 | 说明                                                         |
      | --------------- | ------ | -------- | ------------------------------------------------------------ |
      | sourceDir  | 无     | 否       | 表示sftp服务器里的绝对路径                                 |
      | tempDestination    | 无     | 否       | sftp文件存在的本地的临时路径                               |
      | tempDestinationDirPermission  | rw-rw----     | 是       | 表示临时路径的文件权限 |
      | hdfsDir | 无     | 否       |  | 上到到hdfs的路径 |
      | filterByTime | false | 是 | 可填true/false, 过滤sharefolder上的文件时，是否按文件修改时间过滤，如果是true,则会除了文件名正则匹配，也需要文件修改时间在[dataRangeStart, dataRangeEnd)之间 |
      | timezone | GMT+8 | 是 |  ETL Framework所在服务器的时区            |
      
      示例
      ```sql
      -- step=1
      -- source=mount
      --  fileNamePattern=[\s\S]*.csv.\w*
      --  sourceDir=/mnt/ltg/Archive
      --  tempDestinationDir=/data1/shared_folder/tmp
      --  hdfsDir=hdfs:///data/ltg
      -- target=variables
      -- checkPoint=false
      -- dateRangeInterval=0
      ```
  
### hdfs 文件

  该 step 会读取并解析文件内容，返回一个 DataFrame 以供后续使用。

  hdfs 文件类 step 都有一部分相同的基本参数，如下：

  | 参数名称        | 默认值 | 是否可空 | 说明                                                         |
    | --------------- | ------ | -------- | ------------------------------------------------------------ |
  | dataSourceType  | 无     | 否       | 可选值：hdfs、json、csv、excel。                             |
  | configPrefix    | 无     | 否       | 需读取文件的系统连接配置前缀。                               |
  | fileDir         | 无     | 事       | 如果在 application.properties 中已配置过 `${configPrefix}.${dataSourceType}.dir` 参数，则此处无需再次指定 `fileDir`，如果重复指定，会已 `fileDir` 指定的目录为准。 |
  | fileNamePattern | .*     | 否       | 文件匹配正则。分三段，格式为："(前缀)(文件名主体)(后缀)"。<br />一般情况下前后缀都给个空括号即可。<br />个别情况下读取文件时需要根据同目录下的标志文件判断文件是否已写出完整。例如：正则为：`()(a\.txt)(\.OK)`，则目录下只存在 `a.txt` 文件时不会读取此文件，只有目录下同时存在 `a.txt.OK` 文件时才会去读取文件 `a.txt`。 |
  | deleteSource    | false  | 否       | 是否在文件复制完成后删除原始文件，默认false，不删除。        |

    - hdfs

      参数：

      | 参数名称          | 默认值 | 是否可空 | 说明                                                         |
          | ----------------- | ------ | -------- | ------------------------------------------------------------ |
      | codecExtension    | 无     | 是       | 需解压后上传到 hdfs 时，指定压缩格式后缀，例如：gz、zip。默认为空，不解压。 |
      | separator         | 无     | 是       | 按分隔符解析文本时使用的分割符。separator 与 fieldLengthConfig 只配置一个即可。 |
      | fieldLengthConfig | 无     | 是       | 按字段长度解析文本时各字段的长度配置（字节数）。separator 与 fieldLengthConfig 只配置一个即可。 |
      | strictColumnNum   | false  | 否       | 文件列数是否需要严格一致，默认不需要。例如：源文件包含 30 个字段，目标表需要 20 个字段，非严格模式直接取文件前 20 列，严格模式会过滤掉列数不一致的数据行。 |

      分隔符分割文本示例：

      ```sql
      -- step=1
      -- source=hdfs
      --  configPrefix=test
      --  fileNamePattern=()(a\.test.+\.gz)()
      --  encoding=utf-8
      --  codecExtension=.gz
      --  separator=\|\+\|
      -- target=hive
      --  tableName=test_table
      -- writeMode=overwrite
      ```

      固定字段长度文本示例：

      ```sql
      -- step=1
      -- source=hdfs
      --  configPrefix=test
      --  fileNamePattern=()(a\.test.+\.gz)()
      --  encoding=gbk
      --  codecExtension=
      --  fieldLengthConfig=28,10,3,23,23,23
      --  deleteSource=false
      -- target=hive
      --  tableName=test_table
      -- writeMode=overwrite
      ```

    - json

      参数：

      | 参数名称  | 默认值     | 是否可空 | 说明                                                         |
          | --------- | ---------- | -------- | ------------------------------------------------------------ |
      | multiline | false      | 否       | 一个 json 串是否可能由多行数据组成，默认false，启用此功能性能影响较大，如无必要不建议启用。 |
      | mode      | PERMISSIVE | 是       | 是否启用严格模式，严格模式会严格校验字段数。默认不启用。     |

      示例：

      ```sql
      -- step=1
      -- source=json
      --  configPrefix=test
      --  fileNamePattern=()(test_\d{14}\.json)()
      --  multiline=false
      -- target=temp
      --  tableName=temp
      ```

    - csv

      暂时只是简单实现，全部使用代码中指定的默认参数，无可自定义参数。

      参数：

      | 参数名称    | 默认值 | 是否可空 | 说明                                                         |
          | ----------- | ------ | -------- | ------------------------------------------------------------ |
      | inferSchema | true   | 否       | 是否启用 schema 推测，默认 true。                            |
      | encoding    | utf-8  | 否       | 文件编码。默认 utf-8。                                       |
      | sep         | ,      | 否       | 分隔符，默认英文逗号。                                       |
      | header      | true   | 否       | 文件内是否包含表头，默认包含。                               |
      | selectExpr  | *      | 否       | 每个 csv 需要返回的字段列表，多个字段逗号分隔。使用场景如下：<br />一次性加载多个 csv 文件，此时可以通过指定返回字段避免因为列数、列序不一致而导致的程序报错（ps：列数、列序可以不一致，但是必需的字段必需全部包含）。 |

      示例：

      ```sql
      -- step=1
      -- source=csv
      --  configPrefix=test
      --  fileNamePattern=()(test\.csv)()
      --  selectExpr=field1,field2
      -- target=temp
      --  tableName=temp
      ```

    - excel

      参数：

      | 参数名称                | 默认值              | 是否可空 | 说明                                                         |
          | ----------------------- | ------------------- | -------- | ------------------------------------------------------------ |
      | header                  | true                | 否       | 是否包含表头，默认 true，包含表头。                          |
      | treatEmptyValuesAsNulls | false               | 否       | 是否将空值转换为 null，默认 false，不转换。                  |
      | inferSchema             | false               | 否       | 是否启用结构推断，默认 false，不启用。                       |
      | addColorColumns         | false               | 否       | 是否额外添加列颜色字段，默认 false，不添加。                 |
      | dataAddress             | 无                  | 是       | 数据地址，默认 A1 ，可部分设置，只设置 sheet 页或只设置开始单元格位置都可以。<br />例：'Sheet2'!A1:D3<br />'${sheet页名称}'!${开始单元格位置}:${终止单元格位置}<br />${sheet页下标}!${开始单元格位置}:${终止单元格位置} |
      | timestampFormat         | MM-dd-yyyy HH:mm:ss | 是       | 默认 yyyy-mm-dd hh:mm:ss[.fffffffff]                         |
      | maxRowsInMemory         | 无                  | 是       | 读取超大文档可设置此值，会使用 streaming reader。            |
      | excerptSize             | 无                  | 是       | 结构推断时使用的数据行数，默认 10。                          |
      | workbookPassword        | 无                  | 是       | 文档密码，默认 null，无密码。                                |

      示例：

      ```sql
      -- step=1
      -- source=excel
      --  configPrefix=test
      --  fileNamePattern=()(^BA\.xlsx$)()
      --  dataAddress=0!A1
      --  onlyOneName=true
      -- target=console
      ```

### udf注册

  该 step 在 read 时会将指定类路径下的类加载到内存中。普通的 object 和 class 没什么差别，可以混着用。如果是带参的 class ，需要修改代码支持（pmml即是这种类型）。

  在 write 时将此方法注册为指定名称的 udf 函数，生效范围为当前 SparkSession 的生命周期。

  参数：

  | 参数名称       | 默认值 | 是否可空 | 说明                    |
    | -------------- | ------ | -------- | ----------------------- |
  | dataSourceType | 无     | 否       | 可选值：object、class。 |
  | className      | 无     | 否       | 需要加载的类路径。      |

  示例：

  ```sql
  -- step=1
  -- source=object
  --  className=com.box.datapipeline.udf.DesDecryptUDF
  -- target=udf
  --  methodName=desDecryptHex
  --  udfName=des_decrypt
  
  -- step=2
  -- source=hive
  -- target=hive
  --  tableName=test_table
  -- writeMode=overwrite
  select des_decrypt(trim(a), '11111111', 'GBK') as actnum
  from test_table;
  ```

    - object

    - class

    - pmml

      此处的 pmml 是一种带参的 class 反射，需要在配置中指定 pmml 文件路径。

      参数：

      | 参数名称       | 默认值 | 是否可空 | 说明                                                         |
          | -------------- | ------ | -------- | ------------------------------------------------------------ |
      | dataSourceType | 无     | 否       | 可选值：pmml。                                               |
      | className      | 无     | 否       | 需要加载的类路径，`com.github.sharpdata.sharpetl.spark.udf.PmmlUDF`。 |
      | pmmlFileName   | 无     | 否       | 预测需要使用的模型文件名称。                                 |

      示例：

      ```sql
      -- step=1
      -- source=pmml
      --  className=com.github.sharpdata.sharpetl.spark.udf.PmmlUDF
      --  pmmlFileName=test_pmml.pmml
      -- target=udf
      --  methodName=predict
      --  udfName=predict
      
      -- step=2
      -- source=hive
      -- target=hive
      --  tableName=test_table
      select id,
             predict(
                     struct(
                             *
                         )
                 ) as result
      from temp;
      ```

### bigquery
 
  参数

  | 参数名称       | 默认值 | 是否可空 | 说明                    |
  | -------------- | ------ | -------- | ----------------------- |
  | dataSourceType | 无     | 否       | 值必须为bigquery |
  | system | 无     | 否       | 通过bigquery.${system}.*来获取数据源的配置信息 |

  bigquery在application.properties里的配置信息

  ```properties
  bigquery.test.proxyAddress=localhost:8080
  bigquery.test.parentProject=project1
  bigquery.test.project=project2
  bigquery.test.dataset=main_qa
  bigquery.test.materializationDataset=main_qa
  bigquery.test.credentialsFile=abc3.json
  bigquery.test.viewsEnabled=true
  ```
  
  示例
  ```sql
  -- step=2
  -- source=bigquery
  --  system=test
  -- target=temp
  --  tableName=`login_raw`
  select * from project2.table1
  ```

### http & http_file
 
  参数

  | 参数名称       | 默认值 | 是否可空 | 说明                    |
  | -------------- | ------ | -------- | ----------------------- |
  | connectionName | 无     | 否       | 连接信息 |
  | url | 无     | 否       | 请求地址 |
  | httpMethod | GET     | 否       | 请求方式 |
  | timeout | 无     | 否       | timeout时长 |
  | requestBody | 无     | 否       | 请求体 |
  | fieldName | value     | 否       | 解析response |
  | jsonPath | $     | 否       | 解析response |
  | splitBy | 空字符串     | 否       | 解析response |
  | tempDestinationDir | /tmp     | 否       | 本地临时目录 |
  | hdfsDir | /tmp     | 否       | HDFS保存目录 |

  在application.properties里的配置信息

  ```properties
  your_connection_name.http.header.Authorization=Basic 123456
  your_connection_name.http.proxy.host=localhost
  your_connection_name.http.proxy.port=8080
  ```
  
  示例
  ```sql
  -- step=1
  -- source=http
  --  url=http://localhost:1080/get_workday?satrt=${START_TIME_TIMESTAMP}&end=${START_TIME_TIMESTAMP}
  -- target=temp
  --  tableName=source_data

  -- step=2
  -- source=temp
  --  tableName=source_data
  -- target=temp
  --  tableName=source_data_workday
  -- writeMode=append
  with `workday_temp` as (select explode(from_json(value,
                                                  'struct<Report_Entry:array<struct<a:string,b:string,c:string,d:string,e:string,f:string,g:string,h:string,i:string,j:string,k:string,l:string,m:string,n:string,o:string,p:string,q:string>>>').Report_Entry)
                                    as Report_Entry
                          from `source_data`)
  select Report_Entry.`a` as `a`,
        Report_Entry.`b` as `b`,
        Report_Entry.`c` as `c`,
        Report_Entry.`d` as `d`,
        Report_Entry.`e` as `e`,
        Report_Entry.`f` as `f`,
        Report_Entry.`g` as `g`,
        Report_Entry.`h` as `h`,
        Report_Entry.`i` as `i`,
        Report_Entry.`j` as `j`,
        Report_Entry.`k` as `k`,
        Report_Entry.`l` as `l`,
        Report_Entry.`m` as `m`,
        Report_Entry.`n` as `n`,
        Report_Entry.`o` as `o`,
        Report_Entry.`p` as `p`,
        Report_Entry.`q` as `q`,
        '${YEAR}'  as `year`,
        '${MONTH}' as `month`,
        '${DAY}'   as `day`,
        '${HOUR}'  as `hour`
  from `workday_temp`;
  ```


## Write

输出类型分为以下几类：

1. 返回 DataFrame，基于 DataFrame 做进一步操作。
2. 注册指定内容到 SparkSession 。

详细输出类型如下：

### temp

  该 step 的计算结果将注册为当前 SparkSession 生命周期内可用的内存临时表，可在后续 Read 类型为 `hive` 或 `temp` 的 step 中直接在 sql 中调用。

  参数：

  | 参数名称       | 默认值 | 是否可空 | 说明                           |
    | -------------- | ------ | -------- | ------------------------------ |
  | dataSourceType | 无     | 否       | 可选值：temp。                 |
  | tableName      | 无     | 否       | 计算结果注册为临时表时的表名。 |

  示例：

  ```sql
  -- step=1
  -- source=temp
  -- target=temp
  --  tableName=temp_table
  select 1 as a;
  
  -- step=2
  -- source=temp
  -- target=temp
  select *
  from temp_table;
  ```

### hive

  该 step 的计算结果将写入到目标 hive 表，默认采用动态分区的方式。

  参数：

  | 参数名称       | 默认值 | 是否可空 | 说明                                 |
    | -------------- | ------ | -------- | ------------------------------------ |
  | dataSourceType | 无     | 否       | 可选值：hive。                       |
  | tableName      | 无     | 否       | 计算结果写出到 hive 时的 hive 表名。 |
  | writeMode      | 无     | 否       | 可选值：overwrite、append。          |

  示例：

  ```sql
  -- step=1
  -- source=temp
  -- target=hive
  --  tableName=temp_table
  select 1 as a,
         2 as b;
  ```

### 命令行输出

  该 step 的计算结果将输出到 console ，最多显示前 10000 行。

  参数：

  | 参数名称       | 默认值 | 是否可空 | 说明              |
    | -------------- | ------ | -------- | ----------------- |
  | dataSourceType | 无     | 否       | 可选值：console。 |

  示例：

  ```sql
  -- step=1
  -- source=temp
  -- target=console
  select 1 as a;
  ```

### variables

  该 step 的执行结果为只有一行（可以为多列）数据的 DataFrame，每个字段都将被设置为全局变量，可以在后续 step 的 sql 中直接引用。

  参数：

  示例：

  ```sql
  -- step=1
  -- source=temp
  -- target=variables
  select '1' as a,
         '2' as b;
  
  -- step=2
  -- source=temp
  -- target=hive
  --  tableName=temp_table
  select '${a}' as a,
         '${b}' as b,
         c
  from test_table;
  ```

- udf

  该 step 将会把 Read 时加载到内存中的类的指定方法注册为 udf。

  参数：

  | 参数名称       | 默认值 | 是否可空 | 说明                     |
    | -------------- | ------ | -------- | ------------------------ |
  | dataSourceType | 无     | 否       | 可选值：udf。            |
  | methodName     | 无     | 否       | 需注册为 udf 的方法名    |
  | udfName        | 无     | 否       | 注册成 udf 后的 udf 名称 |

  示例：

  ```sql
  -- step=1
  -- source=pmml
  --  className=com.box.datapipeline.udf.PmmlUDF
  --  pmmlFileName=test_rand_model.pmml
  -- target=udf
  --  methodName=predict
  --  udfName=predict
  
  -- step=2
  -- source=hive
  -- target=temp
  --  tableName=temp1
  select cast(1.0 as float) as x1,
         cast(1.0 as float) as x2,
         cast(1.0 as float) as x3;
  
  -- step=3
  -- source=temp
  -- target=temp
  --  tableName=temp2
  select predict(
                 struct(
                         x1,
                         x2,
                         x3
                     )
             ) as target,
         x1,
         x2,
         x3
  from temp1;
  ```

### do_nothing

  该 step 只执行 Read 部分的操作，Write 部分不做任何处理。

  示例：

  ```sql
  -- step=1
  -- source=hive
  -- target=do_nothing
  truncate table test_table;
  ```

- jdbc 类

  jdbc 类的 Write 基于统一的接口，用法一致。支持以下输出模式：

    - append

      追加写数据到目标表，如果存在主键冲突则报错。

    - upsert

      根据主键对目标表做 upsert 操作。

    - delete

      根据主键对目标表对 delete 操作。

    - execute

      直接执行该 step 中的 sql。适合做一些 DDL 之类的操作，例如：truncat table、create table、create index 等。

  参数：

  | 参数名称       | 默认值 | 是否可空 | 说明                                                         |
    | -------------- | ------ | -------- | ------------------------------------------------------------ |
  | dataSourceType | 无     | 否       | 可选值：mysql、oracle、postgres、ms_sql_server。             |
  | dbName         | 无     | 否       | 如果指定了 dbName，会使用 application.properties 配置文件中以指定的 dbName 开头的配置项去读数据，不指定默认无前缀。 |
  | tableName      | 无     | 否       | 目标表名称。                                                 |
  | writeMode      | 无     | 否       | 可选值：append、upsert、delete、execute。                    |

  示例：

  ```sql
  -- step=1
  -- source=temp
  -- target=postgres
  --  dbName=test
  --  tableName=test_table
  -- writeMode=upsert
  select *
  from temp;
  ```

    - mysql
    - oracle
    - postgres
    - ms_sql_server

### ElasticSearch

  es 操作需在 application.properties 中配置基本连接信息，格式为 `es.*`，示例：

  ```properties
  es.nodes=localhost:9200,localhost:9201,localhost:9202
  es.net.http.auth.user=elastic
  es.net.http.auth.pass=123456
  es.batch.write.refresh=false
  es.index.auto.create=true
  es.batch.size.entries=5000
  es.batch.size.bytes=15mb
  es.write.operation=index
  es.spark.dataframe.write.null=true
  es.nodes.wan.only=true
  ```

  参数：

  | 参数名称       | 默认值 | 是否可空 | 说明                                                         |
    | -------------- | ------ | -------- | ------------------------------------------------------------ |
  | dataSourceType | 无     | 否       | 可选值：es。                                                 |
  | tableName      | 无     | 否       | 需写入的 es index 名称。                                     |
  | primaryKeys    | 无     | 是       | es 中的主键（不设置的话会自动生成），update 模式必须设置，否则无法更新。 |

  示例：

  ```sql
  -- step=1
  -- source=hive
  -- target=es
  --  tableName=test_index
  --  primaryKeys=id
  select id,
         name
  from test_table;
  ```

### kudu

  | 参数名称       | 默认值 | 是否可空 | 说明                            |
    | -------------- | ------ | -------- | ------------------------------- |
  | dataSourceType | 无     | 否       | 可选值：kudu。                  |
  | tableName      | 无     | 否       | 需写入的 kudu 表（库名.表名）。 |

  ```sql
  -- step=1
  -- source=hive
  -- target=impala_kudu
  --  tableName=test.test_table
  -- writeMode=upsert
  select *
  from test_table;
  ```

### impala_kudu

  impale_kudu 与 kudu 查询的逻辑基本一致，都是通过 kudu spark 去操作数据，但不同的是 impale_kudu 是操作由 impala 托管的 kudu 表。由于 impala 默认会在建表时给表名加一个前缀，因此我们在 impala 中操作 kudu 表时使用的表名可能是 'test_db.test_table'，但通过 kudu 直接去操作时表名可能就是 `impala::test_db.test_table` 了。

  因此，需要在 application.properties 配置文件中额外配置 `kudu.table.prefix` 参数。

  ```properties
  kudu.master=localhost:7051
  kudu.table.prefix=impala::
  ```

  | 参数名称       | 默认值 | 是否可空 | 说明                   |
    | -------------- | ------ | -------- | ---------------------- |
  | dataSourceType | 无     | 否       | 可选值：impala_kudu。  |
  | dbName         | 无     | 否       | 需写入的 kudu 表库名。 |
  | tableName      | 无     | 否       | 需写入的 kudu 表表名。 |

  示例：

  ```sql
  -- step=1
  -- source=hive
  -- target=impala_kudu
  --  dbName=test
  --  tableName=test_table
  -- writeMode=upsert
  select *
  from test_table;
  ```

### 文件传输类

  此类操作的数据源格式为文件（ftp、scp、hdfs），输出类型也是文件，整个过程不需要解析文件内容，只需完成文件传输。

  参数：

  | 参数名称       | 默认值 | 是否可空 | 说明                     |
    | -------------- | ------ | -------- | ------------------------ |
  | dataSourceType | 无     | 否       | 可选值：scp、ftp、hdfs。 |
  | filePath       | 无     | 否       | 文件输出路径。           |

    - scp

      | 参数名称     | 默认值 | 是否可空 | 说明                           |
          | ------------ | ------ | -------- | ------------------------------ |
      | configPrefix | 无     | 否       | 需读取文件的系统连接配置前缀。 |

      示例：

      ```sql
      -- step=1
      -- source=hdfs
      --  configPrefix=test
      --  fileNamePattern=()(a\.test.+)()
      -- target=scp
      --  configPrefix=test
      --  filePath=/test
      -- writeMode=overwrite
      ```

    - ftp

      | 参数名称     | 默认值 | 是否可空 | 说明                           |
          | ------------ | ------ | -------- | ------------------------------ |
      | configPrefix | 无     | 否       | 需读取文件的系统连接配置前缀。 |

      示例：

      ```sql
      -- step=1
      -- source=hdfs
      --  configPrefix=test
      --  fileNamePattern=()(a\.test.+)()
      -- target=ftp
      --  configPrefix=test
      --  filePath=/test
      -- writeMode=overwrite
      ```

    - hdfs

      示例：

      ```sql
      -- step=1
      -- source=ftp
      --  configPrefix=test
      --  fileNamePattern=()(a\.test\..+)(OK)
      --  deleteSource=false
      -- target=hdfs
      --  configPrefix=test
      -- writeMode=overwrite
      ```

### hdfs 文件

  此类 step 数据源格式为结构化数据（各种类型的表），整个输出过程需要将源数据按照指定规则处理好并最终输出。

  hdfs 文件类 step 都有一部分相同的基本参数，如下：

  | 参数名称       | 默认值 | 是否可空 | 说明                                               |
    | -------------- | ------ | -------- | -------------------------------------------------- |
  | dataSourceType | 无     | 否       | 可选值：hdfs、csv。                                |
  | filePath       | 无     | 否       | 文件输出路径。                                     |
  | writeMode      | 无     | 否       | 可选值：overWrite、append。一般都使用 overWrite 。 |

    - hdfs

      参数：

      | 参数名称          | 默认值   | 是否可空 | 说明                                                         |
          | ----------------- | -------- | -------- | ------------------------------------------------------------ |
      | Encoding          | utf-8    | 否       | 输出文件的编码格式，默认：utf-8。                            |
      | codecExtension    | 空字符串 | 是       | 需压缩后上传到 hdfs 时，指定压缩格式后缀，例如：gz、zip。默认为空，不压缩。 |
      | separator         | 无       | 是       | 按指定分隔符拼接文本时使用的分割符。separator 与 fieldLengthConfig 只配置一个即可。 |
      | fieldLengthConfig | 无       | 是       | 按固定字段长度拼接文本时各字段的长度配置（字节数）。separator 与 fieldLengthConfig 只配置一个即可。 |

      示例：

      ```sql
      -- step=3
      -- source=hive
      -- target=hdfs
      --  configPrefix=test
      --  filePath=/test.txt
      --  encoding=gbk
      --  fieldLengthConfig=18,4
      -- writeMode=overwrite
      select a,
             b
      from test_table;
      ```

    - csv

      示例：

      ```sql
      -- step=3
      -- source=hive
      -- target=csv
      --  filePath=/test.csv
      -- writeMode=overwrite
      select a,
             b
      from test_table;
      ```


