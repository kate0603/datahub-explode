insert into public.dwd_role_sub_order
select app_key
    ,cp_point
    ,logical_region_id
    ,role_id
from sup_order_payment
where log_time >= '2021-01-01 00:00:00'
order by logical_region_id
limit 100;
