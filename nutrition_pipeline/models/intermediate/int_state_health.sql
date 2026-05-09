with cdc as (
    select * from {{ ref('stg_cdc')}}
),

filtered as (
    select 
        year,
        state_abbr,
        state_name,
        topic,
        question,
        data_value_type,
        data_value,
        low_confidence_limit,
        high_confidence_limit,
        sample_size,
        stratification_category,
        stratification
    from cdc
    where topic in (
        'Obesity / Weight Status',
        'Physical Activity',
        'Fruits and Vegetables'
    )
    and stratification = 'Total'
)

select * from filtered