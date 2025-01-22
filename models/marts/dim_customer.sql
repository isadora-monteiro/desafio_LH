with
    customer as (
        select 
            id_cliente
            , id_pessoa
            , id_loja
            , id_territorio_vendas
            , case
                when id_loja is null  then 'B2C'
                else 'B2B'
            end as tipo_cliente
        from {{ ref('stg_sales_customer')}}
    ),
    person as (
        select 
            id_entidade_negocio
            , tipo_pessoa
            , nm_tipo_pessoa
            , nm_nome
            , nm_sbnome  
            , (nm_nome ||' '|| nm_sbnome) as nm_nome_sbnome    
        from {{ ref('stg_person_person')}}
    ),
    store as (
        select 
              id_entidade_negocio
            , nm_entidade_negocio
            , id_vendedor      
        from {{ ref('stg_sales_store')}}
    ),
    customer_joined as (
        select 
            c.id_cliente
            , c.tipo_cliente
            , p.tipo_pessoa
            , case
                when s.nm_entidade_negocio is null then p.nm_nome_sbnome
                else s.nm_entidade_negocio
            end as nm_cliente
            --, nm_entidade_negocio
            --, p.nm_tipo_pessoa
            , c.id_territorio_vendas    
            , s.id_vendedor  
            --, p.nm_nome
            --, p.nm_sbnome    
        from customer c
        left join person p on c.id_pessoa = p.id_entidade_negocio
        left join store s on c.id_loja = s.id_entidade_negocio
    ),
    customer_with_sk as (
        select
            {{ numeric_surrogate_key(['id_cliente']) }} as sk_cliente
            , *
        from customer_joined
    )

select 
    sk_cliente
    , tipo_cliente       
    , nm_cliente
    , id_vendedor 
    , id_territorio_vendas     
from customer_with_sk



