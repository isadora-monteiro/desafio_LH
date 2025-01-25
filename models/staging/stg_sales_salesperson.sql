with
    source_salesperson as (
        select
            cast(businessentityid as int) as id_vendedor
            , territoryid as id_territorio_vendedor   
            , commissionpct as perc_comissao  
        from {{ source('sales', 'salesperson') }}
    )

select *
from source_salesperson