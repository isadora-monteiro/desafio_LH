with
    source_stateprovince as (
        select
              stateprovinceid as id_provincia
            , stateprovincecode as cod_provincia
            , countryregioncode as cod_regiao
            , isonlystateprovinceflag as flag_unico_estado
            , name as nome_provincia
            , territoryid as id_territorio_vendas       
        from {{ source('raw_sap_aw', 'stateprovince') }}
    )

select *
from source_stateprovince