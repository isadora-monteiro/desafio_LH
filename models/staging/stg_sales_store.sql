with
    source_store as (
        select
              businessentityid as id_entidade_negocio
            , cast(name as varchar) as nm_entidade_negocio 
            , salespersonid as id_vendedor
        from {{ source('raw_sap_aw', 'store') }}
    )

select *
from source_store