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
            , *
        from order_details
    )

select *
from order_details_with_sk
