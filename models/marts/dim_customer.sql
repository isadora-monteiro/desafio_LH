with
    customer as (
        select 
              id_cliente
            , id_pessoa
            , id_loja
            , id_territorio_vendas            
        from {{ ref('stg_sales_customer')}}
    ),
    person as (
        select 
              id_entidade_negocio
            , nm_nome
            , nm_sbnome      
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
            , c.id_pessoa
            , c.id_loja
            , c.id_territorio_vendas            
            , s.nm_entidade_negocio
            , s.id_vendedor  
            , p.nm_nome
            , p.nm_sbnome    
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
    , id_cliente
    , id_pessoa
    , id_loja
    , id_territorio_vendas            
    , nm_entidade_negocio
    , id_vendedor  
    , nm_nome
    , nm_sbnome  
from customer_with_sk



