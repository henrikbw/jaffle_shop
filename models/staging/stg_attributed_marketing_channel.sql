with source as (

    select * from {{ ref('raw_attributed_marketing_channel') }}

),

renamed as (
    select
        id as marketing_channel_id,
        order_id,
        marketing_channel
    from source
)

select * from renamed