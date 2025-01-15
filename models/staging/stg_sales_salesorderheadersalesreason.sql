with
    source_salesorderheadersalesreason as (
        select
              salesorderid as id_pedido
            , salesreasonid as id_motivo_venda
        from {{ source('raw_sap_aw', 'salesorderheadersalesreason') }}
    )

select *
from source_salesorderheadersalesreason 