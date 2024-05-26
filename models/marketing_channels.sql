with attributed_marketing_channel as (

    select * from {{ ref('stg_attributed_marketing_channel') }}

),

marketing_channel_cost as (

    select * from {{ ref('stg_marketing_channel_cost') }}

),

orders as (

    select * from {{ ref('orders') }}

),

joined as (
    select 
        year(order.order_date) as year,
        order.order_id,
        attributed_marketing_channel.marketing_channel,
        order.amount,
        marketing_channel_cost.marketing_channel_cost,
        order.amount - marketing_channel_cost.marketing_channel_cost as marketing_profit -- assuming marketing_channel_cost is per order
    from orders
    left join attributed_marketing_channel
        on orders.order_id = attributed_marketing_channel.order_id
    left join marketing_channel_cost
        on attributed_marketing_channel.marketing_channel = marketing_channel_cost.marketing_channel
),

final as (
    select
        year, -- not necessary as every order is 2018, but can be grouped to give an impression about the channels development
        marketing_channel,
        count(distinct order_id) as orders_count, -- number of orders achieved per channel
        count(distinct customer_id) as customers_count -- number of distinct customers per channel
        sum(amount) as total_order_amount, -- revenue per channel 
        sum(marketing_channel_cost) as total_marketing_cost, -- marketing cost per channel
        sum(marketing_profit) as total_marketing_profit, -- total profit when marketing cost is subtracted per channel
        avg(markering_profit) as avg_marketing_profit -- avg marketing profit per channel

    from joined
    group by year, marketing_channel
)

-- assuming marketing channel cost is per sale
-- other options
-- status set to completed - not necessary as a return is assumed to say more about the products than the sale

select * from final