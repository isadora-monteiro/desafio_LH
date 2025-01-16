with
    source_person as (
        select
              businessentityid as id_entidade_negocio
            , firstname as nm_nome
            , lastname as nm_sbnome 
        from {{ source('raw_sap_aw', 'person') }}
    )

select *
from source_person