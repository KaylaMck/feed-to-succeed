with source as (
    select * from {{ source ('raw', 'census_acs') }}
),

renamed as (
    select
        STATE                                   as state_fips,
        cast(B17001_001E as integer)            as poverty_population_evaluated,
        cast(B17001_002E as integer)            as below_poverty_population,
        cast(B01003_001E as integer)            as total_population,
        cast(B03002_003E as integer)            as white_population,
        cast(B03002_004E as integer)            as black_population,
        cast(B03002_012E as integer)            as hispanic_population
    from source
)

select * from renamed