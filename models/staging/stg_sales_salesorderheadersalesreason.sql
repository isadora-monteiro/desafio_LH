with
    source_salesorderheadersalesreason as (
        select
              salesorderid as id_pedido
            , salesreasonid as id_motivo_venda
        from {{ source('sales', 'salesorderheadersalesreason') }}
    )

select *
from source_salesorderheadersalesreason 