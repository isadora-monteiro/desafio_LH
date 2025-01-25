with
    source_product as (
        select
            cast(productid as int) as id_produto
            , productsubcategoryid as id_subcategoria
            , name as nome_produto
            , cast(finishedgoodsflag as int) as flag_vendavel
            , round(standardcost, 2) as custo_unit 
            , round(listprice, 2) as preco_unit
        from {{ source('production', 'product') }}
    )

select *
from source_product 




