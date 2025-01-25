with
    source_productsubcategory as (
        select
            productsubcategoryid as id_subcategoria
            , productcategoryid as id_categoria
            , name as nome_subcategoria
        from {{ source('production', 'productsubcategory') }}
    )

select 
    id_subcategoria
    , id_categoria
    , nome_subcategoria
from source_productsubcategory 