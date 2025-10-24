{{
    config(
        materialized='table',
        table_type='iceberg',
        format='parquet',
        location_root='s3://luis-leon-demo-bucket/',
    )
}}

with

source as (

    select * from {{ source('ecom', 'raw_orders') }}

),

renamed as (

    select

        ----------  ids
        id as order_id,
        store_id as location_id,
        customer as customer_id,

        ---------- numerics
        subtotal as subtotal_cents,
        tax_paid as tax_paid_cents,
        order_total as order_total_cents,
        CAST((subtotal / 100) as decimal(8,2)) as subtotal,
        CAST((tax_paid / 100) as decimal(8,2)) as tax_paid,
        CAST((order_total / 100) as decimal(8,2)) as order_total,

       --{{ cents_to_dollars('subtotal') }} as subtotal,
        --{{ cents_to_dollars('tax_paid') }} as tax_paid,
        --{{ cents_to_dollars('order_total') }} as order_total,

        ---------- timestamps
        {{ dbt.date_trunc('day','ordered_at') }} as ordered_at

    from source

)

select * from renamed
