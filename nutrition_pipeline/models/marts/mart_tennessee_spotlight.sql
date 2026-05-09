with nutrition as (
    select * from {{ ref('mart_state_nutrition_overview') }}
),

health as (
    select * from {{ ref('int_state_health') }}
),

academics as (
    select * from {{ ref('int_state_academic') }}
),

tn_and_neighbors as (
    select 
        n.state_name,
        n.state_abbr,
        n.region,
        n.poverty_rate,
        n.nslp_participation_rate,
        n.sbp_participation_rate,
        n.total_meal_participation_2024,
        n.total_population
    from nutrition n
    where n.state_abbr in ('TN', 'KY', 'GA', 'AL', 'VA', 'NC', 'MS', 'AR')
),

with_academics as (
    select 
        t.*,
        a.year,
        a.math_avg_score,
        a.reading_avg_score
    from tn_and_neighbors t
    left join academics a on t.state_abbr = a.state_abbr
),

final as (
    select 
        wa.*,
        h.data_value as obesity_rate
    from with_academics wa
    left join health h 
        on wa.state_abbr = h.state_abbr
        and h.question = 'Percent of students in grades 9-12 who have obesity'
        and h.stratification = 'Total'
        and h.year in (2019, 2022, 2024)
)

select * from final