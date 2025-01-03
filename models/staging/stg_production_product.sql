with
    source_product as (
        select
            cast(productid as int) as id_produto
            , name as nome_produto
            , standardcost as custo_padrao_produto     
        from {{ source('raw_sap_aw', 'product') }}
    )

select *
from source_product 




