create table job_log
(
    job_id           bigint auto_increment primary key,
    job_name         varchar(128) charset utf8 not null,
    job_period       int                                not null,
    job_schedule_id  varchar(128) charset utf8 not null,
    data_range_start varchar(128) charset utf8 null,
    data_range_end   varchar(128) charset utf8 null,
    job_start_time   datetime null,
    job_end_time     datetime null,
    status           varchar(32) charset utf8 not null comment 'job status: SUCCESS,FAILURE,RUNNING',
    create_time      datetime default CURRENT_TIMESTAMP not null comment 'log create time',
    last_update_time datetime default CURRENT_TIMESTAMP not null comment 'log update time',
    incremental_type varchar(64) null,
    current_file     varchar(512) charset utf8 null,
    application_id   varchar(64) charset utf8 null,
    project_name     varchar(64) charset utf8 null
) charset = utf8;

create table quality_check_log
(
    id               bigint auto_increment
        primary key,
    job_id           bigint                             not null,
    job_schedule_id  varchar(64) charset utf8 not null comment 'job schedule id(job_name + period)',
    `column`         varchar(64) charset utf8 not null comment 'issue column name',
    data_check_type  varchar(64) charset utf8 not null,
    ids              text charset utf8 not null comment 'issue data primary key, concat by `, `, multiple primary key will be concat by `__`',
    error_type       varchar(16) charset utf8 not null comment 'warn/error',
    warn_count       bigint null,
    error_count      bigint null,
    create_time      datetime default CURRENT_TIMESTAMP not null comment 'log create time',
    last_update_time datetime default CURRENT_TIMESTAMP not null comment 'log update time'
) charset = utf8;

create table step_log
(
    job_id        bigint      not null,
    step_id       varchar(64) not null,
    status        varchar(32) not null,
    start_time    datetime    not null,
    end_time      datetime null,
    duration      int(11) unsigned not null,
    output        text        not null,
    source_count  bigint null,
    target_count  bigint null,
    success_count bigint null comment 'success data count',
    failure_count bigint null comment 'failure data count',
    error         text null,
    source_type   varchar(32) null,
    target_type   varchar(32) null,
    primary key (job_id, step_id)
) charset = utf8;

