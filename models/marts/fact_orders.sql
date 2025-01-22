with
    orders as (
        select 
            id_pedido
            , data_pedido
            , flag_pedido_online
            , id_cliente
            , id_vendedor
            , id_territorio
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
            , {{ numeric_surrogate_key(['id_territorio']) }} as sk_territorio
            , {{ numeric_surrogate_key(['id_endereco']) }} as sk_endereco
            , *
        from orders
    ),
    salesperson as (
        select 
            sk_vendedor
            , id_vendedor
            , id_territorio_vendedor 
            , perc_comissao      
        from {{ ref('dim_salesperson')}}
    ),
    cost_total as (
        select 
            sk_pedido
            , sum(custo_unit_dim) as custo_unit_dim
            , sum(custo_total_dim) as custo_total_dim
            , sum(preco_unit_dim) as preco_unit_dim
            , sum(preco_unit_order) as preco_unit_order
            , sum(preco_total_dim) as preco_total_dim
            , sum(preco_total_order ) as preco_total_order
        from {{ ref('fact_order_details')}}
        group by sk_pedido
    ),
    orders_joined as (
        select
            o.sk_pedido
            , o.sk_cliente
            , o.sk_vendedor
            , o.sk_territorio
            , o.sk_endereco
            --, o.id_pedido
            --, o.id_cliente
            --, s.id_vendedor
            --, o.id_territorio
            --, o.id_endereco
            , o.data_pedido
            , o.flag_pedido_online
            , c.custo_unit_dim
            , c.custo_total_dim
            , c.preco_unit_dim
            , c.preco_unit_order
            , c.preco_total_dim
            , c.preco_total_order
            , o.subtotal
            , s.perc_comissao 
            , round((o.subtotal * s.perc_comissao), 2) as comissao
            , o.taxa
            , o.frete
            , o.total
        from orders_with_sk o 
        full join salesperson s on o.sk_vendedor = s.sk_vendedor
        left join cost_total c on o.sk_pedido = c.sk_pedido
    )


select 
    sk_pedido
    , sk_cliente
    , sk_vendedor
    , sk_territorio
    , sk_endereco
    , data_pedido
    , flag_pedido_online
    , custo_unit_dim
    , custo_total_dim
    , preco_unit_dim
    , preco_unit_order 
    , preco_total_dim
    , preco_total_order
    , subtotal 
    , perc_comissao
    , comissao
    , taxa 
    , frete 
    , total
    , (total - (custo_total_dim + comissao + taxa + frete)) as margem_contribuicao
from orders_joined
