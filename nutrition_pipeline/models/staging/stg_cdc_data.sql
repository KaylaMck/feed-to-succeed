with source as (
    select * from {{ source('raw', 'cdc_data') }}
),

renamed as (
    select
        cast("yearstart" as integer)                as year,
        "locationabbr"                              as state_abbr,
        "locationdesc"                              as state_name,
        "datasource"                                as data_source,
        "topic"                                     as topic,
        "question"                                  as question,
        "data_value_type"                           as data_value_type,
        try_cast("data_value" as float)             as data_value,
        try_cast("low_confidence_limit" as float)   as low_confidence_limit,
        try_cast("high_confidence_limit" as float)  as high_confidence_limit,
        try_cast("sample_size" as integer)          as sample_size,
        "stratificationcategory1"                   as stratification_category,
        "stratification1"                           as stratification
    from source
    where "datasource" = 'Youth Risk Behavior Surveillance System'
    and "locationabbr" != 'US'
)

select * from renamed