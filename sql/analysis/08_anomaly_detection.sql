-- ============================================================
-- Detección de Anomalías Estadísticas
--
-- Objetivo: Identificar ordenes, productos y períodos
-- con comportamiento inusual usando estadística descriptiva.
--
-- Técnica: Z-Score y regla de 2σ (2 desviaciones estándar))
-- → Si un valor se aleja más de 2σ del promedio,
-- es estadísticamente inusual (ocurre solo ~5% del tiempo)
--
-- Análisis incluidos:
-- 1. Órdenes con valor de pago anómalamente alto
-- 2. Productos con caída de venta semanal > 30%
-- 3. Días con volumen de órdenes inusual
-- 4. Órdenes con tiempo de entrega anómalamente largo
--
-- Tablas usadas:
-- fact_orders → verificar fechas y estados de orden
-- fact_order_payments → verificar montos de pago de ordenes
-- fact_order_items → verificar cantidad de tipo de productos en ordenes
-- fact_order_reviews → verificar satisfacción de cliente
-- dim_date → verificar días de la semana y fechas especiales
-- ============================================================

SET search_path TO ecommerce;

-- ============================================================
-- ANÁLISIS 1: Órdenes con valor de pago anómalamente alto
-- Detecta posibles fraudes o errores de carga
-- ============================================================
WITH payment_stats AS (
    SELECT
        order_id,
        SUM(payment_value) AS total_payment,
        -- Media y desviación estándar sobre TODAS las órdenes
        AVG(SUM(payment_value)) OVER () AS mean_payment,
        STDDEV(SUM(payment_value)) OVER () AS stddev_payment
    FROM fact_order_payments
    GROUP BY order_id
),
zscore_payments AS (
    SELECT
        order_id,
        total_payment,
        mean_payment,
        stddev_payment,
        -- Z-Score = cuántas desviaciones estándar se aleja del promedio
        -- Z > 2 → anómalo alto
        -- Z < -2 → anómalo bajo
        (total_payment - mean_payment) / NULLIF(stddev_payment, 0) AS z_score
    FROM payment_stats
)
SELECT
    zp.order_id,
    o.order_purchase_timestamp,
    o.order_status,
    zp.total_payment,
    ROUND(zp.mean_payment::NUMERIC, 2)AS mean_payment,
    ROUND(zp.stddev_payment::NUMERIC, 2) AS stddev_payment,
    ROUND(zp.z_score::NUMERIC, 2) AS z_score,
    CASE
        WHEN zp.z_score > 3 THEN 'Crítico - revisar urgente'
        WHEN zp.z_score > 2 THEN 'Anómalo alto'
        WHEN zp.z_score < -2 THEN 'Anómalo bajo'
    END AS anomaly_type
FROM zscore_payments zp JOIN fact_orders o ON zp.order_id = o.order_id
WHERE ABS(zp.z_score) > 2
ORDER BY ABS(zp.z_score) DESC;

-- ============================================================
-- ANÁLISIS 2: Productos con caída de ventas > 30% semana a semana
-- Detecta problemas de stock, competencia o calidad
-- Aquí es donde estraría la IA para explicar la causa
-- ============================================================
WITH weekly_sales AS (
    SELECT
        i.product_id,
        p.product_category_name,
        DATE_TRUNC('week', o.order_purchase_timestamp) AS week_start,
        COUNT(DISTINCT i.order_id) AS orders_count,
        SUM(i.price) AS revenue
    FROM fact_order_items i
    JOIN fact_orders o ON i.order_id = o.order_id
    JOIN dim_products p ON i.product_id = p.product_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY i.product_id, p.product_category_name, week_start
),

sales_with_lag AS (
    SELECT
        product_id,
        product_category_name,
        week_start,
        orders_count,
        revenue,

        -- LAG trae el valor de la semana anterior del mismo periodo (si existe)
        LAG(orders_count) OVER(PARTITION BY product_id ORDER BY week_start) as prev_week_orders,
        LAG(revenue) OVER(PARTITION BY product_id ORDER BY week_start) as prev_week_revenue
    FROM weekly_sales
),

sales_with_change AS (
    SELECT
        *,
        -- Variación porcentual semana a semana
        ROUND(
            (orders_count - prev_week_orders) * 100 / NULLIF(prev_week_orders, 0), 2
        ) AS orders_pct_change,

        ROUND(
            (revenue - prev_week_revenue) * 100 / NULLIF(prev_week_revenue, 0), 2
        ) AS revenue_pct_change
    FROM sales_with_lag
    WHERE prev_week_orders IS NOT NULL
)

SELECT
    product_id,
    product_category_name,
    week_start,
    prev_week_orders,
    orders_count,
    orders_pct_change,
    prev_week_revenue,
    revenue,
    revenue_pct_change,
    '⚠️ Caída significativa' AS flag
FROM sales_with_change
WHERE orders_pct_change < -30 -- cayó más del 30%
AND prev_week_orders >= 5 -- filtra productos con muy bajo volumen (ruido)
ORDER BY orders_pct_change ASC;

-- ============================================================
-- ANÁLISIS 3: Días con volumen de órdenes inusualmente alto o bajo
-- Detecta picos (campañas, errores de carga) o valles (problemas operativos)
-- ============================================================
WITH daily_orders AS (
    SELECT
        order_purchase_timestamp,
        COUNT(*) AS daily_count
    FROM fact_orders
    WHERE order_status NOT IN ('canceled', 'unavailable')
    GROUP BY order_purchase_timestamp
)
stats AS (
    SELECT
        order_purchase_timestamp,
        daily_count,
        AVG(daily_count) OVER() AS mean_daily,
        STDDEV(daily_count) OVER() AS stddev_daily,
    FROM daily_orders
)
SELECT
    order_purchase_timestamp,
    daily_count,
    ROUND(mean_daily::NUMERIC, 2) AS mean_daily,
    ROUND(stddev_daily::NUMERIC, 2) AS stddev_daily,
    ROUND((daily_count - mean_daily) / NULLIF(stddev_daily, 0), 2) AS z_score,
    CASE
        WHEN (daily_count - mean_daily) / NULLIF(stddev_daily, 0) > 2
            THEN 'Pico inusual - Posible campaña o error'
        WHEN (daily_count - mean_daily) / NULLIF(stddev_daily, 2) < -2
            THEN 'Valle inusual - Posible problema operativo'
    END AS anomaly_label
FROM stats
WHERE ABS((daily_count - mean_daily) / NULLIF(stddev_daily, 2)) > 2
ORDER BY z_score DESC;

-- ============================================================
-- ANÁLISIS 4: Órdenes con tiempo de entrega anómalamente largo
-- Cruzado con review_score bajo para confirmar impacto negativo de satisfacción
-- ============================================================
WITH delivery_times AS (
    SELECT
        o.order_id,
        o.order_purchase_timestamp,
        o.order_delivered_customer_date
        o.order_estimated_delivery_date,
        (o.order_delivered_customer_date - o.order_purchase_timestamp) AS actual_days,
        (o.order_estimated_delivery_date - o.order_purchase_timestamp) AS estimated_days,
        AVG(o.order_delivered_customer_date - o.order_purchase_timestamp) OVER() AS mean_days
        STDDEV(o.order_delivered_customer_date - o.order_purchase_timestamp) OVER() AS stddev_days
    FROM fact_orders o
    WHERE o.order_delivered_customer_date IS NOT NULL
    AND o.order_status IN ('delivered')
)
anomalous_deliveries AS (
    SELECT *,
        ROUND(
            (actual_days - mean_days) / NULLIF(stddev_days, 0), 2
        ) AS z_score
    FROM delivery_times
    WHERE (actual_days - mean_days) / NULLIF(stddev_days, 0) > 2 -- entregas que tardan más de 2σ
)

SELECT
    ad.order_id,
    ad.order_purchase_timestamp,
    ad.actual_days,
    ad.estimated_days,
    (ad.actual_days - ad.estimated_days) AS delay_days,
    ROUND(ad.z_score::NUMERIC, 2) AS z_score,
    r.review_score,
    CASE
        WHEN r.review_score <= 2 THEN '🔴 Entrega tardía + reseña negativa'
        WHEN r.review_score = 3  THEN '🟡 Entrega tardía + reseña neutral'
        ELSE                          '🟢 Entrega tardía pero reseña positiva'
    END AS impact_label
FROM anomalous_deliveries ad
LEFT JOIN fact_order_reviews r ON ad.order_id = r.order_id
ORDER BY ad.z_score DESC;