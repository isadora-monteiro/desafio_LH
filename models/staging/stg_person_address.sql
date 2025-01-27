with
    source_address as (
        select
            addressid as id_endereco
            , addressline1 as endereco
            , city as cidade
            , postalcode as cod_postal
            , stateprovinceid as id_provincia     
        from {{ source('person', 'address') }}
    )

select *
from source_address