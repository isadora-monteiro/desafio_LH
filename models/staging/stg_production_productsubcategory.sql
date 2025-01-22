with
    source_productsubcategory as (
        select
            productsubcategoryid as id_subcategoria
            , productcategoryid as id_categoria
            , name as nome_subcategoria
        from {{ source('raw_sap_aw', 'productsubcategory') }}
    )

select 
    id_subcategoria
    , id_categoria
    , nome_subcategoria
from source_productsubcategory 