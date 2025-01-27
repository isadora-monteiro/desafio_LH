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
            customer.id_cliente
            , customer.tipo_cliente
            , person.tipo_pessoa
            , case
                when store.nm_entidade_negocio is null then person.nm_nome_sbnome
                else store.nm_entidade_negocio
            end as nm_cliente
            , customer.id_territorio_vendas    
            , store.id_vendedor   
        from customer
        left join person on customer.id_pessoa = person.id_entidade_negocio
        left join store on customer.id_loja = store.id_entidade_negocio
    ),
    customer_with_sk as (
        select
            {{ numeric_surrogate_key(['id_cliente']) }} as sk_cliente
            , {{ numeric_surrogate_key(['id_vendedor']) }} as sk_vendedor
            , {{ numeric_surrogate_key(['id_territorio_vendas']) }} as sk_territorio_vendas
            , *
        from customer_joined
    )

select 
    sk_cliente
    , tipo_cliente       
    , nm_cliente
    , sk_vendedor 
    , sk_territorio_vendas     
from customer_with_sk



