create table sharp_etl.job_log
(
    job_id           bigint identity
        constraint PK_JobLog_TransactionID
            primary key,
    workflow_name    nvarchar(128)              not null,
    "period"         int                        not null,
    job_name         nvarchar(128)              not null,
    data_range_start nvarchar(128),
    data_range_end   nvarchar(128),
    job_start_time   datetime,
    job_end_time     datetime,
    status           nvarchar(32)               not null,
    create_time      datetime default getdate() not null,
    last_update_time datetime default getdate() not null,
    load_type        varchar(32),
    log_driven_type  varchar(32),
    "file"           nvarchar(512),
    application_id   nvarchar(64),
    project_name     nvarchar(64),
    runtime_args     nvarchar(512)
)
go

create table sharp_etl.quality_check_log
(
    id               bigint identity
        constraint PK_QCLog_TransactionID
            primary key,
    job_id           bigint                     not null,
    job_name         nvarchar(64)               not null,
    [column]         nvarchar(64)               not null,
    data_check_type  nvarchar(64)               not null,
    ids              nvarchar(max)              not null,
    error_type       nvarchar(16)               not null,
    warn_count       bigint,
    error_count      bigint,
    create_time      datetime default getdate() not null,
    last_update_time datetime default getdate() not null
)
go

create table sharp_etl.step_log
(
    job_id        bigint       not null,
    step_id       varchar(64)  not null,
    status        varchar(32)  not null,
    start_time    datetime     not null,
    end_time      datetime,
    duration      int          not null,
    output        varchar(max) not null,
    source_count  bigint,
    target_count  bigint,
    success_count bigint,
    failure_count bigint,
    error         nvarchar(max),
    source_type   nvarchar(32),
    target_type   nvarchar(32),
    constraint PK_StepLog_TransactionID
        primary key (job_id, step_id)
)
go