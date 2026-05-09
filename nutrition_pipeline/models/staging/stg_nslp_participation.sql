with source as (
    select * from {{ source ('raw', 'nslp_participation') }}
),

renamed as (
    select
        $1                              as state_name,
        try_cast($2 as float)           as participation_2021,
        try_cast($3 as float)           as participation_2022,
        try_cast($4 as float)           as participation_2023,
        try_cast($5 as float)           as participation_2024,
        try_cast($6 as float)           as participation_2025
    from source
),

cleaned as (
    select * from renamed
    where state_name not in (
        'NATIONAL SCHOOL LUNCH PROGRAM: TOTAL PARTICIPATION',
        'Date as of April 10, 2026',
        'State/Territory',
        '  TOTAL',
        'Dept. of Defense',
        'DOD Marines',
        'DOD Navy',
        'DOD Germany',
        'Commonwealth of Northern Mariana Islands',
        'Participation data are nine-month averages; summer months (June-August) are excluded. Participation is based on average daily meals divided by an attendance factor of 0.927. Department of Defense activity represents children of armed forces personnel attending schools overseas. ',
        'All data are subject to revision.',
        '** In SY 2020-2021, many schools served meals through the Summer Food Service Program (SFSP) due to the COVID-19 waivers. The COVID-19 waiver which allowed schools to serve meals through the Seamless Summer Option (SSO) and report separately started in SY 2021-2022. FY 2021 - 2022 data includes meals served through the SSO. '
    )
    and state_name is not null
    and participation_2021 is not null
)

select * from cleaned