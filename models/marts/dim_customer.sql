with
    customer as (
        select *        
        from {{ ref('stg_sales_customer')}}
    )

select *
from customer