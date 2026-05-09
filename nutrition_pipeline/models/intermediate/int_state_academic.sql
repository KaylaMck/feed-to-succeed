with math as (
    select * from {{ ref('stg_nces_math') }}
),

reading as (
    select * from {{ ref('stg_nces_reading') }}
),

joined as (
    select
        math.year,
        math.state_abbr,
        math.state_name,
        math.avg_score                      as math_avg_score,
        reading.avg_score                   as reading_avg_score
    from math
    left join reading
        on math.state_abbr = reading.state_abbr
        and math.year = reading.year
)

select * from joined