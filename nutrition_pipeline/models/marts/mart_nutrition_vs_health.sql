with nutrition as (
    select * from {{ ref('mart_state_nutrition_overview') }}
),

health as (
    select * from {{ ref('int_state_health') }}
),

obesity as (
    select 
        state_abbr,
        year,
        data_value as obesity_rate
    from health
    where question = 'Percent of students in grades 9-12 who have obesity'
),

physical_activity as (
    select 
        state_abbr,
        year,
        data_value as physical_activity_rate
    from health
    where question = 'Percent of students in grades 9-12 who achieve 1 hour or more of moderate-and/or vigorous-intensity physical activity daily'
),

final as (
    select
        n.state_name,
        n.state_abbr,
        n.region,
        n.poverty_rate,
        n.nslp_participation_rate,
        n.sbp_participation_rate,
        o.year,
        o.obesity_rate,
        pa.physical_activity_rate
    from nutrition n
    left join obesity o on n.state_abbr = o.state_abbr
    left join physical_activity pa
        on n.state_abbr = pa.state_abbr
        and o.year = pa.year
    where o.year in (2019, 2022, 2024)
)

select * from final