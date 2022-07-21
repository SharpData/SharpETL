This guide provides a quick guide for commandline `batch-job` 

## Introduction

The command `batch-job` runs all jobs in batch each time and should be noted as one of arguments when running a job. For example, when running a sample job,and the command is as follows:

```bash
# run all job in batch by `spark-submit`
spark-submit --class com.github.sharpdata.sharpetl.spark.Entrypoint spark/build/libs/spark-1.0.0-SNAPSHOT.jar batch-job -f ~/Desktop/sharp-etl-Quick-Start-Guide.xlsx --default-start-time="2021-09-30 00:00:00" --local --once

# run all job locally
./gradlew :spark:run --args="batch-job -f ~/Desktop/sharp-etl-Quick-Start-Guide.xlsx --default-start-time='2021-09-30 00:00:00' --local --once"
```

## Parameters

### common command params

1. `--local`

Declare that the job is running in standalone mode. If `--local` not provided, the job will try running with Hive support enabled.

2. `--release-resource`

The function is to automatically close spark session after job completion.

3. `--skip-running`

When there is a flash crash, use `--skip-running` to set last job status(in running state) as failed and start a new one.

4. `--default-start` / `--default-start-time`

Specify the default start time(eg, 20210101000000)/incremental id of this job. If the command is running for the first time, the default time would be the time set by the argument. If not, the argument would not work.

5. `--once`

It means that the job only run one time(for testing usage). 

6. `--env`

Specify the default env path: local/test/dev/qa/prod running the job.

7. `--property`

Using specific property file, eg `--property=hdfs:///user/admin/etl-conf/etl.properties`

8. `--override`

Overriding config in properties file, eg `--override=etl.workflow.path=hdfs:///user/hive/sharp-etl,a=b,c=d`

### batch-job params

1. `--names`

Specify the names of the job to run.

2. `-f` / `--file`

Specify excel file to run.

3. `--period`

Specify the period of job execution.

4. `-h` / `--help`

Take an example of parameters and its default value is false.
