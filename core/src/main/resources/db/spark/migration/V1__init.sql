create database if not exists `sharp_etl`;

create table `sharp_etl`.job_log
(
    job_id           string,
    workflow_name    string not null,
    `period`         int    not null,
    job_name         string not null,
    data_range_start string,
    data_range_end   string,
    job_start_time   timestamp,
    job_end_time     timestamp,
    status           string not null comment 'job status: SUCCESS,FAILURE,RUNNING',
    create_time      timestamp comment 'log create time',
    last_update_time timestamp comment 'log update time',
    load_type        string,
    log_driven_type  string,
    file             string,
    application_id   string,
    project_name     string,
    runtime_args     string
) using delta;

create table `sharp_etl`.quality_check_log
(
    id               bigint,
    job_id           string      not null,
    job_name         string      not null comment 'job name(workflow_name + period)',
    `column`         string      not null comment 'issue column name',
    data_check_type  string      not null,
    ids              string      not null comment 'issue data primary key, concat by `, `, multiple primary key will be concat by `__`',
    error_type       varchar(16) not null comment 'warn/error',
    warn_count       bigint,
    error_count      bigint,
    create_time      timestamp comment 'log create time',
    last_update_time timestamp comment 'log update time'
) using delta;

create table `sharp_etl`.step_log
(
    job_id        string    not null,
    step_id       string    not null,
    status        string    not null,
    start_time    timestamp not null,
    end_time      timestamp,
    duration      int       not null,
    output        string    not null,
    source_count  bigint,
    target_count  bigint,
    success_count bigint comment 'success data count',
    failure_count bigint comment 'failure data count',
    error         string,
    source_type   string,
    target_type   string
) using delta;

