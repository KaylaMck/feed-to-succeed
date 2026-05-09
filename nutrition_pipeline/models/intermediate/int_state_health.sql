with cdc as (
    select * from {{ ref('stg_cdc_data')}}
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
    'Physical Activity - Behavior',
    'Fruits and Vegetables - Behavior',
    'Sugar Drinks - Behavior'
    )
    and stratification = 'Total'
)

select * from filtered