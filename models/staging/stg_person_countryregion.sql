with
    source_countryregion as (
        select
              countryregioncode as cod_regiao
            , name as nome_regiao 
        from {{ source('raw_sap_aw', 'countryregion') }}
    )

select *
from source_countryregion