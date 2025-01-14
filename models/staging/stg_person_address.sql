with
    source_adress as (
        select
              addressid as id_endereco
            , addressline1 as endereco
            , city as cidade
            , postalcode as cod_postal
            , stateprovinceid as id_provincia     
        from {{ source('raw_sap_aw', 'address') }}
    )

select *
from source_adress