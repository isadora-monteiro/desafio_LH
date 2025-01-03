with
    salesreason as (
        select 
            sohsr.id_pedido
            , sohsr.id_motivo_venda
            , sr.id_nome_motivo_venda 
            , sr.id_tipo_motivo_venda         
        from {{ ref('stg_sales_salesorderheadersalesreason')}} sohsr
        left join {{ ref('stg_sales_salesreason')}} sr
            on sohsr.id_motivo_venda  = sr.id_motivo_venda 
    )

select *
from salesreason