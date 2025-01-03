with
    product as (
        select *        
        from {{ ref('stg_production_product')}}
    )

select *
from product