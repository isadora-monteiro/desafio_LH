with
    orders as (
        select 
            id_pedido
            , data_pedido
            , flag_pedido_online
            , id_cliente
            , id_vendedor
            , id_territorio
            , subtotal
            , taxa
            , frete
            , total     
        from {{ ref('stg_sales_salesorderheader')}}
    ),
    orders_with_sk as (
        select
              {{ numeric_surrogate_key(['id_pedido']) }} as sk_pedido
            , {{ numeric_surrogate_key(['id_cliente']) }} as sk_cliente
            , {{ numeric_surrogate_key(['id_vendedor']) }} as sk_vendedor
            , {{ numeric_surrogate_key(['id_territorio']) }} as sk_territorio
            , *
        from orders
    )

select 
    sk_pedido
    , data_pedido
    , flag_pedido_online
    , sk_cliente
    , sk_vendedor
    , sk_territorio
    , sk_cartao_credito
    , subtotal
    , taxa
    , frete
    , total     
from orders_with_sk
