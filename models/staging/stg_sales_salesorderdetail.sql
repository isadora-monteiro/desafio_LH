with
    source_salesorderdetail as (
        select
            cast(salesorderdetailid as int) as id_pedido_item
            , salesorderid as id_pedido
            , orderqty as quantidade_produto
            , productid as id_produto
            , specialofferid as id_oferta
            , round(unitprice, 2) as preco_unit
            , unitpricediscount as preco_unit_desconto
        from {{ source('sales', 'salesorderdetail') }}
    )

select *
from source_salesorderdetail 