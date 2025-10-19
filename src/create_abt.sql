with 
-- CTE de vendas: agrupa vendas por mês e seller
-- Cria uma data no formato YYYY-MM-01 para agrupamento mensal
vendas_mensais as (
    SELECT
        date(strftime('%Y-%m', t1.order_approved_at) || '-01') AS dt_venda,
        t2.seller_id,
        1 AS venda
    FROM tb_orders AS t1
    LEFT JOIN tb_items AS t2 
        ON t1.order_id = t2.order_id
    WHERE 
        t1.order_status = 'delivered'
        AND t1.order_approved_at IS NOT NULL
        AND t2.seller_id IS NOT NULL
    GROUP BY 
        date(strftime('%Y-%m', t1.order_approved_at) || '-01'),
        t2.seller_id
),

-- CTE de churn: calcula flag de churn para cada seller em cada data de referência
-- Considera churn quando não há vendas nos 3 meses seguintes à data de referência
churn_calculation as (
    SELECT 
        t1.dt_ref,
        t1.seller_id,
        CASE 
            WHEN COUNT(t2.venda) = 0 THEN 1  -- não houve venda → churn
            ELSE 0                           -- houve venda → não churn
        END AS flag_churn_3m
    FROM tb_book_sellers AS t1

    LEFT JOIN vendas_mensais AS t2  -- Usa a CTE de vendas aqui
        ON t1.seller_id = t2.seller_id
        AND date(t2.dt_venda) BETWEEN date(t1.dt_ref) 
                                AND date(t1.dt_ref, '+3 months')

    GROUP BY 
        t1.dt_ref,
        t1.seller_id
    ORDER BY
        t1.seller_id,
        t1.dt_ref
)

-- Query final: junta o cálculo de churn com os dados completos do book_sellers
SELECT 
    t2.*,
    t1.flag_churn_3m
FROM churn_calculation AS t1  -- Usa a CTE de churn aqui
LEFT JOIN tb_book_sellers AS t2
    ON t1.seller_id = t2.seller_id 
    AND t1.dt_ref = t2.dt_ref;