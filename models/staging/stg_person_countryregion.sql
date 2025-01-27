with
    source_countryregion as (
        select
            countryregioncode as cod_regiao
            , name as nome_regiao 
        from {{ source('person', 'countryregion') }}
    )

select *
from source_countryregion