with nslp as (
    select * from {{ ref('stg_nslp') }}
),

sbp as (
    select * from {{ ref('stg_sbp') }}
),

joined as (
    select 
        nslp.state_name,
        nslp.participation_2021             as nslp_participation_2021,
        nslp.participation_2022             as nslp_participation_2022,
        nslp.participation_2023             as nslp_participation_2023,
        nslp.participation_2024             as nslp_participation_2024,
        nslp.participation_2025             as nslp_participation_2025,
        sbp.participation_2021              as sbp_participation_2021,
        sbp.participation_2022              as sbp_participation_2022,
        sbp.participation_2023              as sbp_participation_2023,
        sbp.participation_2024              as sbp_participation_2024,
        sbp.participation_2025              as sbp_participation_2025
    from nslp
    left join sbp on nslp.state_name = sbp.state_name
)

select * from joined