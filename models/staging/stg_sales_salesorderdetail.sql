with
    source_salesorderdetail as (
        select
            salesorderdetailid as id_pedido_item
            , salesorderid as id_pedido
            , orderqty as quantidade_produto
            , productid as id_produto
            , specialofferid as id_oferta
            , unitprice as preco_unit
            , unitpricediscount as preco_unit_desconto
        from {{ source('raw_sap_aw', 'salesorderdetail') }}
    )

select *
from source_salesorderdetail 