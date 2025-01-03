with
    source_salesreason as (
        select
            cast(salesreasonid as int) as id_motivo_venda
            , cast(name as varchar(50)) as id_nome_venda  
            , cast(reasontype as varchar(50)) as id_tipo_venda   
        from {{ source('raw_sap_aw', 'salesreason') }}
    )

select *
from source_salesreason
