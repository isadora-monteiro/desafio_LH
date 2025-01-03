with
    source_salesreason as (
        select
            cast(salesreasonid as int) as id_motivo_venda
            , name as id_nome_motivo_venda  
            , reasontype as id_tipo_motivo_venda   
        from {{ source('raw_sap_aw', 'salesreason') }}
    )

select *
from source_salesreason
