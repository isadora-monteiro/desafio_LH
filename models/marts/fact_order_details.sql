with
    order_details as (
        select 
              id_pedido_item
            , id_pedido
            , quantidade_produto
            , id_produto
            , id_oferta
            , preco_unit
            , preco_unit_desconto       
        from {{ ref('stg_sales_salesorderdetail')}} 
    ),
    order_details_with_sk as (
        select
              {{ numeric_surrogate_key(['id_pedido_item']) }} as sk_pedido_item
            , {{ numeric_surrogate_key(['id_pedido']) }} as sk_pedido
            , {{ numeric_surrogate_key(['id_produto']) }} as sk_produto
            , *
        from order_details
    ),
    products as (
        select
            sk_produto
            , id_produto
            , id_subcategoria
            , id_categoria
            , nome_produto
            , nome_subcategoria 
            , nome_categoria
            , classificacao
            , custo_unit
            , preco_unit
        from {{ ref('dim_product')}} 
    ),
    order_details_joined as (
        select 
            o.sk_pedido_item
            , o.sk_pedido
            , o.sk_produto
            , o.id_pedido_item
            , o.id_pedido
            , o.quantidade_produto
            , o.id_produto
            , o.id_oferta
            , round( p.custo_unit, 2) as custo_unit_dim
            , round( (o.quantidade_produto * p.custo_unit), 2) as custo_total_dim
            , p.preco_unit as preco_unit_dim
            , o.preco_unit as preco_unit_order
            , o.preco_unit_desconto 
            , round( (o.quantidade_produto * p.preco_unit), 2) as preco_total_dim
            , round( (o.quantidade_produto * o.preco_unit), 2) as preco_total_order
            , p.id_subcategoria
            , p.id_categoria
            , p.nome_produto
            , p.nome_subcategoria 
            , p.nome_categoria
            , p.classificacao
        from order_details_with_sk o
        left join products p on o.sk_produto = p.sk_produto
    )

select 
    sk_pedido_item
    , sk_pedido
    , sk_produto
    --, id_pedido_item
    --, id_pedido
    , quantidade_produto
    --, id_produto
    , id_oferta
    , custo_unit_dim
    , custo_total_dim
    , preco_unit_dim
    , preco_unit_order
    , preco_unit_desconto 
    , preco_total_dim
    , preco_total_order
from order_details_joined
