-- auto-generated definition
create table job_definitions
(
    id                  uuid                     default gen_random_uuid()              not null
        primary key,
    name                varchar(255)                                                    not null
        unique,
    target_url          text                                                            not null,
    http_method         varchar(10)              default 'GET'::character varying       not null
        constraint job_definitions_http_method_check
            check ((http_method)::text = ANY
                   ((ARRAY ['GET'::character varying, 'POST'::character varying, 'PUT'::character varying, 'DELETE'::character varying, 'PATCH'::character varying])::text[])),
    enabled             boolean                  default true                           not null,
    next_execution_time timestamp with time zone                                        not null,
    created_at          timestamp with time zone default now()                          not null,
    updated_at          timestamp with time zone default now()                          not null,
    cron_expression     varchar(100)             default '* * * * *'::character varying not null
);

comment on table job_definitions is 'Registry of all recurring HTTP cron jobs';

comment on column job_definitions.next_execution_time is 'Absolute UTC timestamp of the next scheduled run; updated atomically by the Scheduler after each dispatch';

alter table job_definitions
    owner to cron;

create index idx_jd_next_execution
    on job_definitions (next_execution_time)
    where (enabled = true);

-- #################################################################################################################

-- auto-generated definition
create table job_execution_history
(
    id                uuid                     default gen_random_uuid() not null
        primary key,
    job_id            uuid                                               not null
        references job_definitions
            on delete cascade,
    scheduled_time    timestamp with time zone                           not null,
    actual_start_time timestamp with time zone                           not null,
    delay_ms          bigint generated always as ((EXTRACT(epoch FROM (actual_start_time - scheduled_time)) *
                                                   (1000)::numeric)) stored,
    status            varchar(20)                                        not null
        constraint job_execution_history_status_check
            check ((status)::text = ANY
        ((ARRAY ['SUCCESS'::character varying, 'FAILED'::character varying, 'TIMEOUT'::character varying])::text[])),
    http_status_code  integer,
    response_payload  text,
    created_at        timestamp with time zone default now()             not null
);

comment on table job_execution_history is 'Append-only execution log; one row per job invocation attempt';

comment on column job_execution_history.delay_ms is 'Scheduling drift in milliseconds (actual_start - scheduled). Computed by the DB.';

comment on column job_execution_history.status is 'SUCCESS = 2xx received; FAILED = non-2xx or network error; TIMEOUT = no response within threshold';

alter table job_execution_history
    owner to cron;

create index idx_jeh_job_id_created
    on job_execution_history (job_id asc, created_at desc);

create index idx_jeh_job_delay
    on job_execution_history (job_id, delay_ms)
    where (delay_ms IS NOT NULL);

