with
    source_product as (
        select
            cast(productid as int) as id_produto
            , cast(name as varchar(50)) as nome_produto
            , cast(standardcost as int) as custo_padrao_produto     
        from {{ source('raw_sap_aw', 'product') }}
    )

select *
from source_product 




