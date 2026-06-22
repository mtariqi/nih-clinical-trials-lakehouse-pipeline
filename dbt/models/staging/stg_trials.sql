-- models/staging/stg_trials.sql
-- Standardises raw clinical trial records from the bronze/silver Redshift table.

with source as (
    select * from {{ source('nih_raw', 'trials_raw') }}
),

renamed as (
    select
        nct_id,
        lower(trim(overall_status))                         as overall_status,
        lower(trim(study_type))                             as study_type,
        {{ clean_phase('phase') }}                          as phase,
        brief_title,
        official_title,

        -- Dates
        try_cast(start_date as date)                        as start_date,
        try_cast(completion_date as date)                   as completion_date,
        try_cast(last_update_post_date as date)             as last_update_post_date,

        -- Enrollment
        try_cast(enrollment_count as integer)               as enrollment_count,

        -- Sponsor
        trim(lead_sponsor_name)                             as lead_sponsor_name,
        upper(trim(lead_sponsor_class))                     as lead_sponsor_class,

        -- Location
        trim(location_country)                              as location_country,

        -- Eligibility
        trim(minimum_age)                                   as minimum_age,
        trim(maximum_age)                                   as maximum_age,
        lower(trim(sex))                                    as sex,
        lower(trim(healthy_volunteers))                     as healthy_volunteers,

        -- Metadata
        ingest_date,
        ingest_timestamp

    from source
    where nct_id is not null
      and overall_status is not null
),

deduped as (
    select *,
        row_number() over (
            partition by nct_id
            order by last_update_post_date desc, ingest_timestamp desc
        ) as row_num
    from renamed
)

select * from deduped
where row_num = 1
