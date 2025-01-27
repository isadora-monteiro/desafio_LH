with
    salesreason as (
        select 
            id_motivo_venda
            , nome_motivo_venda  
            , id_tipo_motivo_venda   
        from {{ ref('stg_sales_salesreason')}}
    ),
    salesreason_with_sk as (
        select
            {{ numeric_surrogate_key(['id_motivo_venda']) }} as sk_motivo_venda
            , *
        from salesreason
    )

select 
    sk_motivo_venda
    , nome_motivo_venda  
from salesreason_with_sk