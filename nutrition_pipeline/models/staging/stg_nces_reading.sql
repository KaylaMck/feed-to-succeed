with source as (
    select * from {{ source('raw', 'nces_reading') }}
),

renamed as (
    select
        cast("year" as integer)                     as year,
        "jurisdiction"                              as state_abbr,
        "jurisLabel"                                as state_name,
        "subject"                                   as subject,
        "grade"                                     as grade,
        try_cast("value" as float)                  as avg_score,
        "varValueLabel"                             as student_group,
        cast("isStatDisplayable" as integer)        as is_displayable,
        cast("errorFlag" as integer)                as error_flag
    from source
    where "varValueLabel" = 'All students'
    and "isStatDisplayable" = '1'
    and "errorFlag" = '0'
)

select * from renamed