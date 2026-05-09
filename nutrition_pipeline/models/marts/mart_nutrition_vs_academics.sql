with nutrition as (
    select * from {{ ref('mart_state_nutrition_overview') }}
),

academics as (
    select * from {{ ref('int_state_academic') }}
),

final as (
    select 
        n.state_name,
        n.state_abbr,
        n.region,
        n.poverty_rate,
        n.nslp_participation_rate,
        n.sbp_participation_rate,
        n.total_meal_participation_2024,
        a.year,
        a.math_avg_score,
        a.reading_avg_score
    from nutrition n
    left join academics a on n.state_abbr = a.state_abbr
)

select * from final