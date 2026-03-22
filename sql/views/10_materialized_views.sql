-- ============================================================
-- Vistas Materializadas para KPIs del Dashboard
--
-- ¿Por qué vistas materializadas y no vistas normales?
--
-- Vista Normal → Ejecuta la query CADA VEZ que alguien consulta
-- Vista Materializada → Guarda el resultado físicamente en disco
--                       Se refresca manualmente con REFRESH
--                       El dashboard carga en milisegundos
--
-- ¿Cuándo refrescar?
-- En este proyecto: manualmente después de cada ETL
-- En producción real: con un cron Job o scheduler
--
-- Comando para refrescar todas:
-- REFRESH MATERIALIZED VIEW ecommerce.mv_kpi_general:
-- (repetir para cada vista)
-- ============================================================

SET search_path TO ecommerce;

-- ============================================================
-- VISTA 1: KPIs Generales del Negocio
-- Responde: ¿Cómo está el negocio en números globales?
-- Tarjetas de resumen en la parte superior del dashboard
-- ============================================================

CREATE MATERIALIZED VIEW mv_kpi_general AS
SELECT
    COUNT(DISTINCT o.order_id)              AS total_orders,
    COUNT(DISTINCT c.customer_unique_id)    AS unique_customers,
    COUNT(DISTINCT i.seller_id)             AS active_sellers,
    COUNT(DISTINCT i.product_id)            AS products_sold,
    ROUND(SUM(p.payment_value)::NUMERIC, 2) AS total_revenue,
    ROUND(AVG(p.payment_value)::NUMERIC, 2) AS avg_order_value,
    ROUND(SUM(p.payment_value)::NUMERIC / NULLIF(COUNT(DISTINCT c.customer_unique_id), 0), 2) AS revenue_per_customer,

    -- Tasa de entrega a tiempo
    ROUND(
        COUNT(*)FILTER (
            WHERE o.order_delivered_customer_date <= o.order_estimated_delivery_date
        ) * 100.0 / NULLIF(COUNT(*) FILTER(
            WHERE o.order_delivered_customer_date IS NOT NULL
        ), 0),
        2
    )                                       AS on_time_delivery_rate,

    -- Tasa de cancelación
    ROUND(
        COUNT(*) FILTER(
            WHERE o.order_status = 'canceled'
        ) * 100.0 / NULLIF(COUNT(*), 0),
        2
    )                                       AS cancellation_rate,

    ROUND(AVG(r.review_score)::NUMERIC, 2)   AS avg_review_score

FROM fact_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
JOIN fact_order_items i ON o.order_id = i.order_id
JOIN fact_order_payments p ON o.order_id = p.order_id
LEFT JOIN fact_order_reviews r ON o.order_id = r.order_id;

-- ============================================================
-- VISTA 2: Revenue Mensual
-- Responde: ¿Cómo evoluciona el negocio mes a mes?
-- Gráfico de línea o barras en el dashboard
-- ============================================================

CREATE MATERIALIZED VIEW mv_revenue_monthly AS
SELECT
    DATE_TRUNC('month', o.order_purchase_timestamp)::DATE   AS month,
    TO_CHAR(DATE_TRUNC('month', o.order_purchase_timestamp), 'YYYY-MM')          AS month_label,
    COUNT(DISTINCT o.order_id)                              AS total_orders,
    COUNT(DISTINCT c.customer_unique_id)                    AS unique_customers,
    ROUND(SUM(p.payment_value)::NUMERIC, 2)                 AS revenue,
    ROUND(AVG(p.payment_value)::NUMERIC, 2)                 AS avg_order_value,

    -- Revenue del mes anterior para comparar
    ROUND(LAG(
        SUM(p.payment_value)) OVER(ORDER BY DATE_TRUNC('month', o.order_purchase_timestamp))::NUMERIC, 2)
                                                            AS prev_month_revenue,

    -- Crecimiento mes a mes en %
    ROUND(
        (SUM(p.payment_value) - LAG(SUM(p.payment_value)) OVER(ORDER BY DATE_TRUNC('month', o.order_purchase_timestamp)))
        * 100.0
        /
        NULLIF(LAG(SUM(p.payment_value)) OVER(ORDER BY DATE_TRUNC('month', o.order_purchase_timestamp)), 0)::NUMERIC, 2
    )                                                       AS mom_growth_pct

FROM fact_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
JOIN fact_order_payments p ON o.order_id = p.order_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY DATE_TRUNC('month', o.order_purchase_timestamp)
ORDER BY month;

-- ============================================================
-- VISTA 3: Top categorias de productos por Revenue
-- Responde: ¿Qué categorias generan más dinero?
-- Gráfico de barras horizontales
-- ============================================================

CREATE MATERIALIZED VIEW mv_top_categories AS
SELECT
    p.product_category_name                                 AS category,
    COUNT(DISTINCT i.order_id)                              AS total_orders,
    COUNT(DISTINCT i.product_id)                            AS unique_products,
    ROUND(SUM(i.price)::NUMERIC, 2)                         AS total_revenue,
    ROUND(AVG(i.price)::NUMERIC, 2)                         AS avg_price,
    ROUND(AVG(r.review_score)::NUMERIC, 2)                   AS avg_review_score,

    -- % del revenue total (share de mercado por categoria)
    ROUND(
        SUM(i.price) * 100.0 / SUM(SUM(i.price)) OVER (),
        2
    )                                                       AS revenue_share_pct,

    RANK() OVER(ORDER BY SUM(i.price) DESC) revenue_rank

FROM fact_order_items i
JOIN fact_orders o ON i.order_id = o.order_id
JOIN dim_products p ON i.product_id = p.product_id
LEFT JOIN fact_order_reviews r ON i.order_id = r.order_id
WHERE o.order_status NOT IN ('canceled', 'unavailable')
AND p.product_category_name IS NOT NULL
GROUP BY p.product_category_name
ORDER BY total_revenue DESC;

-- ============================================================
-- VISTA 4: Performance de entrega
-- Responde: ¿Qué tan bien está operando la logistica?
-- KPIs de entrega para el área de operaciones
-- ============================================================

CREATE MATERIALIZED VIEW mv_delivery_performance AS
SELECT
    DATE_TRUNC('month', o.order_purchase_timestamp)::DATE    AS month,
    TO_CHAR(DATE_TRUNC('month', o.order_purchase_timestamp), 'YYYY-MM')          AS month_label,
    COUNT(*)                                                AS total_delivered,

    -- Promedio de días desde compra hasta entrega
    ROUND(
        AVG(o.order_delivered_customer_date - o.order_purchase_timestamp)
        ::NUMERIC, 1
    )                                                       AS avg_delivery_days,

    -- Promedio de días de entrega estimados
    ROUND(
        AVG(o.order_estimated_delivery_date - o.order_purchase_timestamp)
        ::NUMERIC, 1
    )                                                       AS avg_estimated_days,

    -- Órdenes entregadas a tiempo
    COUNT(*) FILTER(
        WHERE o.order_delivered_customer_date <= o.order_estimated_delivery_date
    )                                                       AS on_time_count,

    -- Órdenes entregadas tarde
    COUNT(*) FILTER(
        WHERE o.order_delivered_customer_date > o.order_estimated_delivery_date
    )                                                       AS late_count,

    -- Tasa de entrega a tiempo en %
    ROUND(
        COUNT(*) FILTER(
            WHERE o.order_delivered_customer_date <= o.order_estimated_delivery_date
        ) * 100.0 / NULLIF(COUNT(*), 0), 2
    )                                                       AS on_time_rate

FROM fact_orders o
WHERE o.order_status = 'delivered'
AND o.order_delivered_customer_date IS NOT NULL
GROUP BY DATE_TRUNC('month', o.order_purchase_timestamp)
ORDER BY month;

-- ============================================================
-- VISTA 5: Segmentos RFM resumidos
-- Responde: ¿Cómo está distribuido la base de clientes?
-- Gráfico de dona o barras apiladas
-- ============================================================

CREATE MATERIALIZED VIEW mv_rfm_segments AS
WITH orders_with_payment AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_purchase_timestamp,
        SUM(p.payment_value) AS order_value
    FROM fact_orders o
    JOIN fact_order_payments p ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY o.order_id, o.customer_id, o.order_purchase_timestamp
),

rfm_raw AS (
    SELECT
        c.customer_unique_id,
        DATE '2018-10-18' - MAX(op.order_purchase_timestamp)    AS recency_days,
        COUNT(DISTINCT op.order_id)                             AS frequency,
        SUM(op.order_value)                                     AS monetary
    FROM orders_with_payment op
    JOIN dim_customers c ON op.customer_id = c.customer_id
    GROUP BY c.customer_unique_id
),

rfm_scored AS (
    SELECT
        customer_unique_id,
        recency_days,
        frequency,
        ROUND(monetary::NUMERIC, 2)                             AS monetary,
        NTILE(5) OVER (ORDER BY recency_days ASC)               AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC)                 AS f_score,
        NTILE(5) OVER (ORDER BY monetary DESC)                  AS m_score
    FROM rfm_raw
),

rfm_segmented AS (
    SELECT
        customer_unique_id,
        recency_days,
        frequency,
        monetary,
        (r_score + f_score + m_score)                           AS rfm_total,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3                  THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score <= 2                  THEN 'New customers'
            WHEN r_score <= 2 AND f_score >= 3                  THEN 'At Risk'
            WHEN r_score = 1 AND f_score = 1                    THEN 'Lost'
            WHEN m_score >= 4 AND f_score <= 2                  THEN 'Big Spenders'
            ELSE 'Potential Loyalists'
        END AS segment
    FROM rfm_scored
)

SELECT
    segment,
    COUNT(*)                                                    AS total_customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)           AS percentage,
    ROUND(AVG(monetary)::NUMERIC, 2)                            AS avg_monetary,
    ROUND(AVG(frequency)::NUMERIC, 2)                           AS avg_frequency,
    ROUND(AVG(recency_days)::NUMERIC, 2)                        AS avg_recency_days
FROM rfm_segmented
GROUP BY segment
ORDER BY total_customers DESC;

-- ============================================================
-- ÍNDICES SOBRE LAS VISTAS MATERIALIZADAS
-- Permiten filtrar rápido desde Metabase
-- ============================================================

CREATE INDEX idx_mv_revenue_month   ON mv_revenue_monthly(month);
CREATE INDEX idx_mv_delivery_month  ON mv_delivery_performance(month);
CREATE INDEX idx_mv_categories_rank ON mv_top_categories(revenue_rank);
CREATE INDEX idx_mv_rfm_segment     ON mv_rfm_segments(segment);

-- ============================================================
-- COMANDO PARA REFRESCAR TODAS LAS VISTAS
-- Ejecutar después de cada carga de datos (ETL)
-- ============================================================

-- REFRESH MATERIALIZED VIEW ecommerce.mv_kpi_general;
-- REFRESH MATERIALIZED VIEW ecommerce.mv_revenue_monthly;
-- REFRESH MATERIALIZED VIEW ecommerce.mv_top_categories;
-- REFRESH MATERIALIZED VIEW ecommerce.mv_delivery_performance;
-- REFRESH MATERIALIZED VIEW ecommerce.mv_rfm_segments