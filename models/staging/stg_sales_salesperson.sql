with
    source_salesperson as (
        select
            cast(businessentityid as int) as id_entidade_empresarial
            , cast(territoryid as int) as id_territorio     
        from {{ source('raw_sap_aw', 'salesperson') }}
    )

select *
from source_salesperson