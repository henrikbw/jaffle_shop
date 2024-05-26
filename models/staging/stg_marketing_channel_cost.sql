with source as (

    select * from {{ ref('raw_marketing_channel_cost') }}

),

renamed as (
    select
        marketing_channel
        cost
    from source
)

select * from renamed