with
    source_salesterritory as (
        select
              territoryid as id_territorio_vendas  
            , name as nome_territorio 
            , countryregioncode as cod_regiao
            , group_ as grupo
        from {{ source('sales', 'salesterritory') }}
    )

select *
from source_salesterritory