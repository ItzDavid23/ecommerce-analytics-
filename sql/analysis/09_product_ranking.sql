-- ============================================================
-- Ranking de productos por categoria
-- Análisis de tiempo entre compra y entrega con LAG()
-- ============================================================
--
-- Objetivo:
-- 1. Rankear productos dentro de su categoria por revenue, volumen de ventas y satisfacción del cliente
-- 2. Analizar patrones de entrega usando LAG() para comparar cada orden con la anterior.
--
-- Diferencia entre RANK() y DENSE_RANK():
-- RANK(): 1, 2, 2, 4 → deja hueco después del empate
-- DENSE_RANK(): 1, 2, 2, 3 → no deja hueco, continúa seguido
-- ROW_NUMBER(): 1, 2, 3, 4 → siempre único, sin empates
-- 
-- Tablas usadas:
-- fact_order_items → revenue y volumen de ventas por producto
-- fact_orders → fechas de compra y entrega
-- fact_order_reviews → satisfacción del cliente por producto
-- dim_products → categoría de producto
-- dim_customers → satisfacción por cliente
-- ============================================================

SET search_path TO ecommerce;

-- ============================================================
-- ANÁLISIS 1: Ranking de productos por categoría
-- Métrica principal: revenue total
-- ============================================================

WITH product_metrics AS (
    SELECT
        i.product_id,
        p.product_category_name,
        COUNT(DISTINCT i.order_id) AS total_orders,
        SUM(i.price) AS total_revenue,
        AVG(i.price) AS avg_price,
        AVG(r.review_score) AS avg_review_score,
        COUNT(DISCTINCT r.review_id) AS 
    FROM fact_order_items i JOIN fact_orders o ON i.order_id = o.order_id
    JOIN dim_products p ON i.product_id = p.product_id
    LEFT JOIN fact_order_reviews r ON i.order_id = r.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY i.product_id, p.product_category_name
)

SELECT
    product_id,
    product_category_name,
    total_orders,
    ROUND(total_revenue::NUMERIC, 2) AS total_revenue,
    ROUND(avg_price::NUMERIC, 2) AS avg_price,
    ROUND(avg_review_score::NUMERIC, 2) AS avg_review_score,

    -- RANK: deja hueco si hay empates
    -- EJ: dos productos con mismo revenue → posición 1 y 1, el siguiente sería 3
    RANK() OVER(PARTITION BY product_category_name ORDER BY total_revenue DESC) as rank_by_revenue,

    -- DENSE_RANK: no deja hueco
    -- EJ: dos productos con mismo revenue → posición 1 y 1, el siguiente es 2
    DENSE_RANK() OVER(PARTITION BY product_category_name ORDER BY total_revenue DESC) AS dense_rank_by_revenue,

    -- Ranking por volumen de órdenes (diferente al de revenue)
    RANK() OVER(PARTITION BY product_category_name ORDER BY total_orders DESC) AS rank_by_orders,

    -- Ranking por satisfacción del cliente
    RANK() OVER(PARTITION BY product_category_name ORDER BY avg_review_score DESC NULL LAST) AS rank_by_satisfaction,

    -- Total de productos en la categoría (para saber el denominador)
    COUNT(*) OVER(PARTITION BY product_category_name) AS products_in_category

FROM product_metrics
ORDER BY product_category_name, rank_by_revenue;

-- ============================================================
-- ANÁLISIS 2: Top 3 productos por categoría
-- Filtra solo los mejores de cada categoría
-- Útil para reportes ejecutivos y dahsboards
-- ============================================================
WITH product_metrics AS (
    SELECT
        i.product_id,
        p.product_category_name,
        COUNT(DISTINCT i.order_id) AS total_orders,
        SUM(i.price) AS total_revenue,
        AVG(r.review_score) AS avg_review_score
    FROM fact_order_items i JOIN fact_orders o ON i.order_id = o.order_id
    JOIN dim_products p ON i.product_id = p.product_id
    LEFT JOIN fact_order_reviews r ON i.order_id = r.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY i.product_id, p.product_category_name
),

ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER(PARTITION BY product_category_name ORDER BY total_revenue DESC) AS position
    FROM product_metrics
)

SELECT
    product_category_name,
    position,
    product_id,
    total_orders,
    ROUND(total_revenue::NUMERIC, 2) AS total_revenue,
    ROUND(avg_review_score::NUMERIC, 2) AS avg_review_score
FROM ranked
WHERE position <= 3
ORDER BY product_category_name, position;

-- ============================================================
-- ANÁLISIS 3: Tiempo entre compra y entrega con LAG()
-- Compara cada orden con la anterior del mismo vendedor
-- para detectar si los tiempos están mejorando o empeorando
-- ============================================================

WITH seller_deliveries AS (
    SELECT
        i.seller_id,
        o.order_id,
        o.order_purchase_timestamp,
        o.order_delivered_customer_date,

        -- Días reales de entrega
        (o.order_delivered_customer_date - o.order_purchase_timestamp) AS delivery_days,

        -- Días de la entrega ANTERIOR del mismo vendedor
        LAG(o.order_delivered_customer_date - o.order_purchase_timestamp) OVER(PARTITION BY i.seller_id ORDER BY o.order_purchase_timestamp) AS prev_delivery_days

        -- Fecha de la entrega anterior (para contexto)
        LAG(o.order.purchase_timestamp) OVER(PARTITION BY i.seller_id ORDER BY o.order_purchase_timestamp) AS prev_order_date
    FROM fact_order_items i JOIN fact_orders o ON i.order_id = o.order_id
    WHERE o.order_delivered_customer_date IS NOT NULL
    AND o.order_status = 'delivered'
),

delivery_trend AS (
    SELECT
        seller_id,
        order_id,
        order_purchase_timestamp,
        delivery_days,
        prev_delivery_days,
        (delivery_days - prev_delivery_days) AS days_difference,
        CASE
            WHEN delivery_days < prev_delivery_days THEN '✅ Mejoró'
            WHEN delivery_days > prev_delivery_days THEN '❌ Empeoró'
            WHEN delivery_days = prev_delivery_days THEN '➡️ Igual'
            ELSE 'Primera orden'
        END AS trend
    FROM seller_deliveries
)

SELECT
    seller_id
    order_id
    order_purchase_timestamp,
    delivery_days,
    prev_delivery_days,
    days_difference,
    trend
FROM delivery_trend;
WHERE prev_delivery_days IS NOT NULL
GROUP BY seller_id, order_purchase_timestamp;

-- ============================================================
-- ANÁLISIS 4: Resumen de tendencia de entrega por vendedor
-- Responde: ¿Qué vendedores están mejorando o empeorando?
-- Ideal para cruzar con ranking de satisfacción
-- ============================================================

WITH seller_deliveries AS (
    SELECT
        i.seller_id,
        o.order_id,
        o.order_purchase_timestamp
        (o.order_delivered_customer_date - o.order_purchase_date) AS delivery_days,
        LAG(o.order_delivered_customer_date - o.order_purchase_date) OVER(PARTITION BY i.seller_id ORDER BY o.order_purchase_timestamp) AS prev_delivery_days
    FROM fact_order_items i JOIN fact_orders o ON i.order_id = o.order_id
    WHERE o.order_delivered_customer_date IS NOT NULL
    AND o.order_status = 'delivered'
),

delivery_trend AS (
    SELECT
        seller_id,
        delivery_days,
        prev_delivery_days,
        CASE
            WHEN delivery_days < prev_delivery_days THEN 'mejora'
            WHEN delivery_days > prev_delivery_days THEN 'empeora'
            ELSE 'igual'
        END AS trend
    FROM seller_deliveries
    WHERE prev_delivery_days IS NOT NULL
)

SELECT
    seller_id
    COUNT(*) AS total_deliveries,
    ROUND(AVG(delivery_days)::NUMERIC, 1) AS avg_delivery_days,
    COUNT(*) FILTER (WHERE trend = 'mejora') AS improved,
    COUNT(*) FILTER (WHERE trend = 'empeora') AS worsened,
    COUNT(*) FILTER (WHERE trend = 'igual') AS unchanged,

    -- % de entregas que mejoraron respecto a la anterior
    ROUND(
        COUNT(*) FILTER(WHERE trend = 'mejora') * 100 / COUNT(*),
        1
    ) AS improvement_rate,

    CASE
        WHEN COUNT(*) FILTER (WHERE trend = 'mejora') * 1.0 / COUNT(*) > 0.6
            THEN '🟢 Tendencia positiva'
        WHEN COUNT(*) FILTER (WHERE trend = 'empeora') * 1.0 / COUNT(*) > 0.6
            THEN '🔴 Tendencia negativa'
        ELSE '🟡 Sin tendencia clara'
    END AS seller_trend
FROM delivery_trend
GROUP BY seller_id
HAVING COUNT(*) >= 10 -- Filtra vendedores con pocas ordenes
ORDER BY avg_delivery_days ASC:

-- ============================================================
-- EXPLAIN ANALYZE - Justificación de índices
-- Ejecuta cada bloque por separado en DBeaver
-- para ver el query plan antes y después del índice
-- ============================================================

-- Test 1: Sín índice en order_purchase_timestamp
-- Verás "Seq Scan" → escanea toda la tabla

EXPLAIN ANALYZE
SELECT order_id, order_purchase_timestamp, order_status
FROM fact_orders
WHERE order_purchase_timestamp BETWEEN '2018-01-01' AND '2018-03-31'

-- Crear el índice
-- CREATE INDEX idx_order_purchase_timestamp ON fact_orders(order_purchase_timestamp);

-- TEST 2: Con Índice → verás "Index Scan" o "Bitmap Index Scan"
-- Costo y tiempo de ejecución bajan significativamente

EXPLAIN ANALYZE
SELECT order_id, order_purchase_timestamp, order_status
FROM fact_orders
WHERE order_purchase_timestamp BETWEEN '2018-01-01' AND '2018-03-31'

-- TEST 3: Índice en product_id para joins de order_items

EXPLAIN ANALYZE
SELECT i.product_id, SUM(i.price)
FROM fact_order_items i
JOIN fact_orders o ON i.order_id = o.order_id
WHERE o.order_purchase_timestamp >= '2018-01-01'
GROUP BY i.product_id;

-- ¿Qué buscar en el output de EXPLAIN ANALYZE?
--
-- ✅ "Index Scan"        → usa el índice, eficiente
-- ✅ "Bitmap Index Scan" → usa índice para rangos, eficiente
-- ⚠️  "Seq Scan"         → no usa índice, escanea toda la tabla
-- 📊  "cost=X..Y"        → X = costo de inicio, Y = costo total
-- ⏱️  "actual time=X..Y" → tiempo real en milisegundos
-- 🔁  "rows=N"           → filas estimadas vs reales