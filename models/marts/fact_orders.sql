with
    orders as (
        select 
            id_pedido
            , data_pedido
            , flag_pedido_online
            , id_cliente
            , id_vendedor
            , id_endereco
            , round(subtotal, 2) as subtotal
            , round(taxa, 2) as taxa
            , round(frete, 2) as frete
            , round(total, 2) as total
        from {{ ref('stg_sales_salesorderheader')}}
    ),
    orders_with_sk as (
        select
            {{ numeric_surrogate_key(['id_pedido']) }} as sk_pedido
            , {{ numeric_surrogate_key(['id_cliente']) }} as sk_cliente
            , {{ numeric_surrogate_key(['id_vendedor']) }} as sk_vendedor
            , {{ numeric_surrogate_key(['id_endereco']) }} as sk_endereco
            , *
        from orders
    ),
    salesperson as (
        select 
            sk_vendedor
            , perc_comissao      
        from {{ ref('dim_salesperson')}}
    ),
    orders_joined as (
        select
            o.sk_pedido
            , o.sk_cliente
            , o.sk_vendedor
            , o.sk_endereco
            , o.data_pedido
            , o.flag_pedido_online
            , o.subtotal
            , s.perc_comissao 
            , round((o.subtotal * s.perc_comissao), 2) as comissao
            , o.taxa
            , o.frete
            , o.total
        from orders_with_sk o 
        full join salesperson s on o.sk_vendedor = s.sk_vendedor
    )

select 
    sk_pedido
    , sk_cliente
    , sk_vendedor
    , sk_endereco
    , data_pedido
    , flag_pedido_online
    , subtotal 
    , comissao
    , taxa 
    , frete 
    , total
    --, (total - (custo_total_dim + comissao + taxa + frete)) as margem_contribuicao
from orders_joined
