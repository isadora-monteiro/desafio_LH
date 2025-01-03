with
    salesperson as (
        select *        
        from {{ ref('stg_sales_salesperson')}}
    )

select *
from salesperson