with
    source_salesperson as (
        select
            cast(businessentityid as int) as id_entidade_empresarial
            , territoryid as id_territorio_vendedor     
        from {{ source('raw_sap_aw', 'salesperson') }}
    )

select *
from source_salesperson