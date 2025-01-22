with
    source_productcategory as (
        select
            productcategoryid as id_categoria
            , name as nome_categoria
        from {{ source('raw_sap_aw', 'productcategory') }}
    )

select 
    id_categoria
    , nome_categoria
from source_productcategory 