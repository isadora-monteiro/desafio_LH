with
    source_salesreason as (
        select
            cast(salesreasonid as int) as id_motivo_venda
            , name as nome_motivo_venda  
            , reasontype as id_tipo_motivo_venda   
        from {{ source('sales', 'salesreason') }}
    )

select *
from source_salesreason
