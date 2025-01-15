with
    ordersalesreason as (
        select 
              id_pedido
            , id_motivo_venda
        from {{ ref('stg_sales_salesorderheadersalesreason')}}
    ),
    ordersalesreason_with_sk as (
        select
              {{ numeric_surrogate_key(['id_pedido', 'id_motivo_venda']) }} as sk_motivo_venda_pedido
            , {{ numeric_surrogate_key(['id_pedido']) }} as sk_pedido  
            , {{ numeric_surrogate_key(['id_motivo_venda']) }} as sk_motivo_venda
            , *
        from ordersalesreason
    )

select 
      sk_motivo_venda_pedido
    , sk_pedido
    , sk_motivo_venda
    , id_pedido
    , id_motivo_venda
from ordersalesreason_with_sk
