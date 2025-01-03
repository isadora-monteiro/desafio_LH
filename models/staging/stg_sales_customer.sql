with
    source_customer as (
        select
              cast(customerid as int) as id_cliente
            , cast(personid as int) as id_pessoa
            , cast(storeid as int) as id_loja
            , cast(territoryid as int) as id_territory        
        from {{ source('raw_sap_aw', 'customer') }}
    )

select *
from source_customer