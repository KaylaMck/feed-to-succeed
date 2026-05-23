with nutrition as (
    select * from {{ ref('int_state_nutrition') }}
),

demographics as (
    select * from {{ ref('int_state_demographics') }}
),

crosswalk as (
    select * from {{ ref('state_crosswalk') }}
),

final as (
    select 
        cw.state_name,
        cw.state_abbr,
        cw.region,
        n.nslp_participation_2024,
        n.sbp_participation_2024,
        n.nslp_participation_2024 + n.sbp_participation_2024            as total_meal_participation_2024,
        n.nslp_participation_2021,
        n.sbp_participation_2021,
        n.nslp_participation_2021 + n.sbp_participation_2021            as total_meal_participation_2021,
        d.total_population,
        d.poverty_rate,
        round(
            n.nslp_participation_2024 / nullif(d.school_age_population, 0) * 100, 2
        )                                                               as nslp_participation_rate,
        round(
            n.sbp_participation_2024 / nullif(d.school_age_population, 0) * 100, 2
        )                                                               as sbp_participation_rate
    from crosswalk cw
    left join nutrition n on cw.state_name = n.state_name
    left join demographics d on cw.state_fips = d.state_fips
)

select * from final