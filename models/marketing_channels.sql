with attributed_marketing_channel as (

    select * from {{ ref('stg_attributed_marketing_channel') }}

),

marketing_channel_cost as (

    select * from {{ ref('stg_marketing_channel_cost') }}

),

orders as (

    select * from {{ ref('orders') }}

)

joined as (
    select 
        year(order.order_date) as year,
        order.order_id,
        attributed_marketing_channel.marketing_channel,
        order.amount,
        marketing_channel_cost.marketing_channel_cost,
        order.amount - marketing_channel_cost.marketing_channel_cost as marketing_profit
    from orders
    left join attributed_marketing_channel
        on orders.order_id = attributed_marketing_channel.order_id
    left join marketing_channel_cost
        on attributed_marketing_channel.marketing_channel = marketing_channel_cost.marketing_channel
),

final as (
    select
        year,
        marketing_channel,
        count(distinct order_id) as order_count,
        sum(amount) as total_order_amount,
        sum(marketing_channel_cost) as total_marketing_cost,
        sum(marketing_profit) as total_marketing_profit,
        avg(markering_profit) as avg_marketing_profit

    from joined
    group by year, marketing_channel
)

select * from final