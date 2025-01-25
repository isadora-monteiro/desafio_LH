with
    source_productinventory as (
        select
            cast(productid as int) as id_produto
            , locationid as id_localizacao
            , quantity as volume_produto     
        from {{ source('production', 'productinventory') }}
    )

select *
from source_productinventory 