with census as (
    select * from {{ ref('stg_census_acs') }}
),

calculated as (
    select 
        state_fips,
        total_population,
        poverty_population_evaluated,
        below_poverty_population,
        white_population,
        black_population,
        hispanic_population,
        school_age_population,
        round(
            below_poverty_population / nullif(poverty_population_evaluated, 0) * 100, 2
        )                                                                                       as poverty_rate,
        round
            (white_population / nullif(total_population, 0) * 100, 2)                           as pct_white,
        round
            (black_population/ nullif(total_population, 0) * 100, 2)                            as pct_black,
        round
            (hispanic_population / nullif(total_population, 0) * 100, 2)                        as pct_hispanic,
        round
            (school_age_population / nullif(total_population, 0) * 100, 2)                      as pct_school_age
    from census
)

select * from calculated