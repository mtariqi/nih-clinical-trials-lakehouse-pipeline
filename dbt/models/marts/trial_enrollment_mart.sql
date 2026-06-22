-- models/marts/trial_enrollment_mart.sql
-- Enrollment analytics by phase and condition.
-- Powers the BI dashboard enrollment views and ML feature engineering.

with trials as (
    select * from {{ ref('stg_trials') }}
),

conditions as (
    select * from {{ ref('stg_conditions') }}
),

joined as (
    select
        t.nct_id,
        t.phase,
        t.overall_status,
        t.study_type,
        t.lead_sponsor_class,
        t.location_country,
        t.enrollment_count,
        t.start_date,
        t.completion_date,
        t.last_update_post_date,

        -- Derived duration
        datediff(
            day, t.start_date, coalesce(t.completion_date, current_date)
        )                                                           as study_duration_days,

        -- Completion flag for ML target
        case
            when t.overall_status = 'completed' then 1
            else 0
        end                                                         as is_completed,

        -- Termination flag
        case
            when t.overall_status = 'terminated' then 1
            else 0
        end                                                         as is_terminated,

        c.condition_name,
        c.condition_category

    from trials t
    left join conditions c using (nct_id)
),

aggregated as (
    select
        phase,
        condition_category,
        lead_sponsor_class,
        location_country,

        count(distinct nct_id)                                      as trial_count,
        sum(enrollment_count)                                       as total_enrollment,
        avg(enrollment_count)                                       as avg_enrollment,
        median(enrollment_count)                                    as median_enrollment,
        avg(study_duration_days)                                    as avg_duration_days,
        sum(is_completed) * 100.0 / nullif(count(*), 0)            as completion_rate_pct,
        sum(is_terminated) * 100.0 / nullif(count(*), 0)           as termination_rate_pct

    from joined
    group by 1, 2, 3, 4
)

select * from aggregated
