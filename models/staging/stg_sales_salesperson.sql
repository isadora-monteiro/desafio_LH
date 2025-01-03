with
    source_salesperson as (
        select
            cast(businessentityid as int) as id_entidade_empresarial
            , cast(territoryid as int) as id_territorio     
        from {{ source('raw_sap_aw', 'salesperson') }}
    ),
    sk_salesperson as (
        select
            {{ numeric_surrogate_key(['id_entidade_empresarial']) }} as sk_entidade_empresarial
        from source_salesperson
    )

select *
from sk_salesperson