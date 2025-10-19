with 
-- CTE base: calcula a idade em dias base de cada seller
-- Encontra a primeira venda aprovada de cada seller antes da data de referência
seller_base_age as (
    select 
        t2.seller_id,
        round(max(julianday('{date}') - julianday(t1.order_approved_at)), 2) as idade_dias_base
    from tb_orders as t1
    left join tb_items as t2 on t1.order_id = t2.order_id
    where t1.order_approved_at < '{date}' and t1.order_status = 'delivered'
    group by t2.seller_id
),

-- CTE principal: agrega todas as métricas de vendas, reviews e produtos
-- Filtra pedidos dos últimos 6 meses e com status 'delivered'
seller_metrics as (
    select 
        t2.seller_id,
        count(distinct t5.review_id) as qtd_avaliacoes_recebidas,       
        round(avg(t5.review_score), 2) as review_score_medio,
        round(max(t5.review_score), 2) as review_score_max,
        round(min(t5.review_score), 2) as review_score_min,
        round(cast(sum(case when t5.review_score >= 4 then 1 else 0 end) as float) / 
              nullif(count(t5.review_id), 0), 2) as prop_reviews_positivos,
        
        round(cast(sum(case when t5.review_score < 4 then 1 else 0 end) as float) / 
              nullif(count(t5.review_id), 0), 2) as prop_reviews_negativos,        
        t3.idade_dias_base,
        1 + cast(t3.idade_dias_base / 30 as integer) as idade_meses_base,
        cast(julianday('{date}') - julianday(max(t1.order_approved_at)) as integer) as idade_dias_ult_venda,
        count(distinct strftime('%m', t1.order_approved_at)) as meses_ativado,
        round(cast(count(distinct strftime('%m', t1.order_approved_at)) as float) / min(1 + cast(t3.idade_dias_base / 30 as integer), 6), 2) as perc_tempo_ativado,
        sum(case when julianday(t1.order_estimated_delivery_date) < julianday(t1.order_delivered_customer_date) then 1 else 0 end) as qtd_atraso,
        sum(case when julianday(t1.order_estimated_delivery_date) < julianday(t1.order_delivered_customer_date) then 1 else 0 end) / count(distinct t2.order_id) as prop_atraso,
        round(avg(case when julianday(t1.order_estimated_delivery_date) < julianday(t1.order_delivered_customer_date) 
                  then julianday(t1.order_delivered_customer_date) - julianday(t1.order_estimated_delivery_date) 
                  else 0 end), 2) as media_dias_atraso,        
        round(avg(julianday(t1.order_estimated_delivery_date) - julianday(t1.order_purchase_timestamp)), 0) as med_dias_entrega_estimada,
        round(sum(t2.price), 2) as receita_tot,
        count(distinct t2.order_id) as qtd_vendas,
        round(avg(t2.price), 2) as preco_medio_produto,
        round(avg(t2.freight_value), 2) as frete_medio,
        round(sum(t2.freight_value), 2) as receita_frete_total,
        round(sum(t2.freight_value) / nullif(sum(t2.price), 0), 2) as proporcao_frete_sobre_vendas,        
        round(sum(t2.price) / count(distinct t2.order_id), 2) as tkt_med_venda,
        round(sum(t2.price) / min(1 + cast(t3.idade_dias_base / 30 as integer), 6), 2) as tkt_med_venda_mes,
        round(sum(t2.price) / count(distinct strftime('%m', t1.order_approved_at)), 2) as tkt_med_venda_mes_ativado,
        count(t2.product_id) as qtd_prod_vend,
        count(distinct t2.product_id) as qtd_prod_dst_vend,
        round(sum(t2.price) / count(t2.product_id), 2) as tkt_med_venda_prod,
        count(t2.product_id) / count(distinct t2.order_id) as med_prod_por_vendas,
        sum(case when product_category_name = 'cama_mesa_banho' then 1 else 0 end) as qtd_cama_mesa_banho,
        sum(case when product_category_name = 'beleza_saude' then 1 else 0 end) as qtd_beleza_saude,
        sum(case when product_category_name = 'esporte_lazer' then 1 else 0 end) as qtd_esporte_lazer,
        sum(case when product_category_name = 'moveis_decoracao' then 1 else 0 end) as qtd_moveis_decoracao,
        sum(case when product_category_name = 'informatica_acessorios' then 1 else 0 end) as qtd_informatica_acessorios,
        sum(case when product_category_name = 'utilidades_domesticas' then 1 else 0 end) as qtd_utilidades_domesticas,
        sum(case when product_category_name = 'relogios_presentes' then 1 else 0 end) as qtd_relogios_presentes,
        sum(case when product_category_name = 'telefonia' then 1 else 0 end) as qtd_telefonia,
        sum(case when product_category_name = 'ferramentas_jardim' then 1 else 0 end) as qtd_ferramentas_jardim,
        sum(case when product_category_name = 'automotivo' then 1 else 0 end) as qtd_automotivo,
        sum(case when product_category_name = 'brinquedos' then 1 else 0 end) as qtd_brinquedos,
        sum(case when product_category_name = 'cool_stuff' then 1 else 0 end) as qtd_cool_stuff,
        sum(case when product_category_name = 'perfumaria' then 1 else 0 end) as qtd_perfumaria,
        sum(case when product_category_name = 'bebes' then 1 else 0 end) as qtd_bebes,
        sum(case when product_category_name = 'eletronicos' then 1 else 0 end) as qtd_eletronicos,
        sum(case when product_category_name = 'papelaria' then 1 else 0 end) as qtd_papelaria,
        sum(case when product_category_name = 'fashion_bolsas_e_acessorios' then 1 else 0 end) as qtd_fashion_bolsas_e_acessorios,
        sum(case when product_category_name = 'pet_shop' then 1 else 0 end) as qtd_pet_shop,
        sum(case when product_category_name = 'moveis_escritorio' then 1 else 0 end) as qtd_moveis_escritorio,
        sum(case when product_category_name NOT IN (
            'cama_mesa_banho', 'beleza_saude', 'esporte_lazer', 'moveis_decoracao', 
            'informatica_acessorios', 'utilidades_domesticas', 'relogios_presentes', 
            'telefonia', 'ferramentas_jardim', 'automotivo', 'brinquedos', 'cool_stuff', 
            'perfumaria', 'bebes', 'eletronicos', 'papelaria', 'fashion_bolsas_e_acessorios', 
            'pet_shop', 'moveis_escritorio'
        ) then 1 else 0 end) as qtd_outros        

    from tb_orders as t1
    left join tb_items as t2 on t1.order_id = t2.order_id
    left join seller_base_age as t3 on t2.seller_id = t3.seller_id  -- Usa a CTE aqui
    left join tb_products as t4 on t2.product_id = t4.product_id
    left join tb_reviews as t5 on t1.order_id = t5.order_id

    where t1.order_approved_at between date('{date}', '-6 months') and '{date}'
      and t1.order_status = 'delivered'
    group by t2.seller_id
)

-- Query final: junta as métricas dos sellers com informações geográficas
select
    '{date}' as dt_ref,
    t2.seller_city,
    t2.seller_state,
    t1.*
from seller_metrics as t1  -- Usa a CTE principal aqui
left join tb_sellers as t2 on t1.seller_id = t2.seller_id;