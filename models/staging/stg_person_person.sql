with
    source_person as (
        select
              businessentityid as id_entidade_negocio
            , persontype as tipo_pessoa
            , case
                when persontype = 'SC' then 'Store Contact'
                when persontype = 'IN' then 'Individual (retail) customer'
                when persontype = 'SP' then 'Sales person'
                when persontype = 'EM' then 'Employee (non-sales)'
                when persontype = 'VC' then 'Vendor contact'
                when persontype = 'GC' then 'General contact'
            end as nm_tipo_pessoa
            , cast(firstname as varchar) as nm_nome
            , cast(lastname as varchar) as nm_sbnome 
        from {{ source('person', 'person') }}
    )

select *
from source_person