select t1.*,

       case when t2.seller_id is null then 1 else 0 end as flag_churn_m3

from tb_book_sellers as t1

-- select para verificar churn em 3 meses apos o periodo base
left join (
    select distinct t2.seller_id

    from tb_orders as t1

    left join tb_items as t2
    on t1.order_id = t2.order_id

    where order_approved_at between '2017-04-01'
    and '2017-07-01'
    and t1.order_status = 'delivered'
) as t2
on t1.seller_id = t2.seller_id