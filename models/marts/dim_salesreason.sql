with
    --salesreason as (
    --    select 
    --          id_pedido
    --        , id_motivo_venda
    --    from {{ ref('stg_sales_salesorderheadersalesreason')}}
    --),
    salesreason as (
        select 
              id_motivo_venda
            , nome_motivo_venda  
            , id_tipo_motivo_venda   
        from {{ ref('stg_sales_salesreason')}}
    ),
    --salesreasonjoined as (
    --    select 
    --          sor.id_pedido
    --        , sor.id_motivo_venda
    --        , r.nome_motivo_venda 
    --        , r.id_tipo_motivo_venda         
    --    from salesreason sor
    --    left join reason r on sor.id_motivo_venda  = r.id_motivo_venda 
    --),
    salesreason_with_sk as (
        select
            {{ numeric_surrogate_key(['id_motivo_venda']) }} as sk_motivo_venda
            , *
        from salesreasonjoined
    )

select *
from salesreason_with_sk