create table if not exists `sharp_etl`.job_log
(
    job_id           string,
    workflow_name    string not null,
    `period`         int    not null,
    job_name         string not null,
    data_range_start string,
    data_range_end   string,
    job_start_time   TIMESTAMP(9),
    job_end_time     TIMESTAMP(9),
    status           string not null comment 'job status: SUCCESS,FAILURE,RUNNING',
    create_time      TIMESTAMP(9) comment 'log create time',
    last_update_time TIMESTAMP(9) comment 'log update time',
    load_type        string,
    log_driven_type  string,
    file             string,
    application_id   string,
    project_name     string,
    runtime_args     string,
    PRIMARY KEY (job_id) NOT ENFORCED
);

create table if not exists `sharp_etl`.quality_check_log
(
    id               string,
    job_id           string      not null,
    job_name         string      not null comment 'job name(workflow_name + period)',
    `column`         string      not null comment 'issue column name',
    data_check_type  string      not null,
    ids              string      not null comment 'issue data primary key, concat by `, `, multiple primary key will be concat by `__`',
    error_type       string not null comment 'warn/error',
    warn_count       bigint,
    error_count      bigint,
    create_time      TIMESTAMP(9) comment 'log create time',
    last_update_time TIMESTAMP(9) comment 'log update time',
    PRIMARY KEY (id) NOT ENFORCED
);

create table if not exists `sharp_etl`.step_log
(
    job_id        string    not null,
    step_id       string    not null,
    status        string    not null,
    start_time    TIMESTAMP(9) not null,
    end_time      TIMESTAMP(9),
    duration      int       not null,
    output        string    not null,
    source_count  bigint,
    target_count  bigint,
    success_count bigint comment 'success data count',
    failure_count bigint comment 'failure data count',
    error         string,
    source_type   string,
    target_type   string,
    PRIMARY KEY (job_id, step_id) NOT ENFORCED
);

