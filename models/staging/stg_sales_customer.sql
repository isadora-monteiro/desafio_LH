with
    source_customer as (
        select
              cast(customerid as int) as id_cliente
            , cast(personid as int) as id_pessoa
            , cast(storeid as int) as id_loja
            , cast(territoryid as int) as id_territory        
        from {{ source('raw_sap_aw', 'customer') }}
    ),
    sk_customer as (
        select
            {{ numeric_surrogate_key(['id_cliente']) }} as sk_cliente
        from source_customer
    )

select *
from sk_customer