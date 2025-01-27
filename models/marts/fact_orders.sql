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
            , metodo_pagamento
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
            orders_with_sk.sk_pedido
            , orders_with_sk.sk_cliente
            , orders_with_sk.sk_vendedor
            , orders_with_sk.sk_endereco
            , orders_with_sk.data_pedido
            , orders_with_sk.flag_pedido_online
            , orders_with_sk.subtotal
            , salesperson.perc_comissao 
            , round((orders_with_sk.subtotal * salesperson.perc_comissao), 2) as comissao
            , orders_with_sk.taxa
            , orders_with_sk.frete
            , orders_with_sk.total
            , orders_with_sk.metodo_pagamento
        from orders_with_sk 
        full join salesperson on orders_with_sk.sk_vendedor = salesperson.sk_vendedor
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
    , metodo_pagamento
from orders_joined
