with attributed_marketing_channel as (

    select * from {{ ref('stg_attributed_marketing_channel') }}

),

marketing_channel_cost as (

    select * from {{ ref('stg_marketing_channel_cost') }}

),

orders as (

    select * from {{ ref('orders') }}

),

orders_per_channel as (
    select 
        attributed_marketing_channel.marketing_channel,
        count(distinct orders.order_id) as orders_count,
        count(distinct orders.customer_id) as customers_count,
        sum(orders.amount) as revenue
    from orders
    left join attributed_marketing_channel
        on orders.order_id = attributed_marketing_channel.order_id

    group by attributed_marketing_channel.marketing_channel
),

orders_cost_per_channel as (
    select
        orders_per_channel.marketing_channel,
        orders_per_channel.orders_count,
        orders_per_channel.customers_count,
        orders_per_channel.revenue,
        marketing_channel_cost.marketing_channel_cost,
        orders_per_channel.revenue - marketing_channel_cost.marketing_channel_cost as profit

    from orders_per_channel
    left join marketing_channel_cost 
        on orders_per_channel.marketing_channel = marketing_channel_cost.marketing_channel
)

-- assuming marketing channel cost is total per channel
-- other options
-- status set to completed - not necessary as a return is assumed to say more about the products than the sale
-- group by year to see development - but only 2018 orders here

select * from orders_cost_per_channel