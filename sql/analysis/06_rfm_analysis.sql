-- ============================================================
-- Analysis RFM: Recency, Frecuency, Monetary
-- Segmentación de clientes usando CTEs encadenadas
--
-- Objetivo: Identificar clientes valiosos,
-- clientes en riesgo de abandono y clientes nuevos.
--
-- Tablas usadas:
--  fact_orders         → verificar las fechas de compra y estado
--  fact_order_payments → verificar montos de compra
--  dim_customers       → información del cliente
-- ============================================================

SET search_path TO ecommerce;

-- ============================================================
-- Paso 1: Fecha de referencia
-- Usamos el día siguiente al último pedido en el dataset (current_date en producción)
-- ============================================================

-- SELECT MAX(order_purchase_timestamp) AS last_order_date FROM fact_orders;
-- Supongamos que el último pedido fue el 2024-05-31, entonces la fecha de referencia será el 2024-06-01

-- ============================================================
-- ANÁLISIS RFM COMPLETO
-- ============================================================

WITH
-- CTE 1: Una fila por orden con su valor total de pago
-- Necesario porque fact_order_payments puede tener
-- múltiples filas con una misma orden_id (pagos en cuotas o mixtos)

orders_with_payment AS (
    SELECT
    o.order_id,
    o.customer_id,
    o.order_purchase_timestamp,
    SUM(p.payment_value) AS order_value
    FROM fact_orders o
    JOIN fact_order_payments p ON o.order_id = p.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable') -- Excluir pedidos no completados
    GROUP BY o.order_id, o.customer_id, o.order_purchase_timestamp
),

-- CTE 2: Métricas brutas por cliente
-- Recency → tiempo desde su última compra
-- Frequency → cuántas ordenes ha realizado
-- Monetary → cuánto ha gastado en total

rfm_raw AS (
    SELECT
    c.customer_unique_id,
    MAX(order_purchase_timestamp) AS last_purchase_date, -- Fecha de la última compra
    DATE '2018-10-18' - MAX(order_purchase_timestamp) AS recency_days, -- Días desde la última compra
    COUNT(DISTINCT op.order_id) AS frequency_orders, -- Numero de órdenes realizadas
    SUM(op.payment_value) AS monetary
    FROM orders_with_payment op
    JOIN dim_customers c ON op.customer_id = c.customer_id
    GROUP BY c.customer_unique_id
),

-- CTE 3: Scores del 1 al 5 usando NTILE
-- NTILE divide los clientes en 5 grupos iguales
-- Recency: score 5 = compró más recientemente (mejor)
-- Frequency: score 5 = compró más veces (mejor)
-- Monetary: score 5 = gastó más (mejor)

rfm_scored AS (
    SELECT
        customer_unique_id,
        last_purchase_date,
        recency_days,
        frequency_orders,
        ROUND(monetary::NUMERIC, 2) AS monetary,

        -- Recency invertida: menos días = mejor score
        NTILE(5) OVER(ORDER BY recency_days ASC) as recency_score,
        NTILE(5) OVER(ORDER BY frequency_orders DESC) as frecuency_score,
        NTILE(5) OVER(ORDER BY monetary DESC) as monetary_score
    FROM rfm_raw
),

-- CTE 4: Score total y segmento de negocio
-- El segmento convierte los números en etiquetas
-- accionables para equipos de marketing

rfm_segmented AS (
    SELECT
        customer_unique_id,
        last_purchase_date,
        recency_days,
        frequency_orders,
        monetary,
        recency_score,
        frequency_score,
        monetary_score,
        (recency_score + frequency_score + monetary_score) AS rfm_total,

        CASE
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4  THEN 'Champions' -- compraron reciente, seguido y gastaron mucho
            WHEN recency_score >= 3 AND frequency_score >= 3 THEN 'Loyal customers' -- compran con regularidad
            WHEN recency_score >= 4 AND frequency_score >= 2 THEN 'New customers' -- compraron reciente pero pocas veces
            WHEN recency_score >= 2 AND frequency_score >= 3 THEN 'At risk'-- compraban seguido pero llevan tiempo sin comprar
            WHEN recency_score >= 1 AND frequency_score >= 1 THEN 'Lost' -- no compran hace mucho y compraron poco
            WHEN monetary_score >= 4 AND frequency_score >= 2 THEN 'Big Spenders' -- gastan mucho pero no compran seguido
            ELSER 'Potential Loyalist' -- potenciales clientes leales
        END AS segment
    FROM rfm_scored
)

-- ============================================================
-- Resultado final: Clientes con su segmento RFM
-- ============================================================

SELECT
    customer_unique_id,
    last_purchase_date,
    recency_days,
    frequency_orders,
    monetary,
    recency_score,
    frequency_score,
    monetary_score,
    rfm_total.
    segment
FROM rfm_segmented
ORDER BY rfm_total DESC;

-- ============================================================
-- RESUMEN EJECUTIVO: distribución de segmentos
-- Usarlo para dashboard o para reportar a negocio
-- ============================================================

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
        MAX(order_purchase_timestamp) AS last_purchase_date,
        DATE '2018-10-18' - MAX(order_purchase_timestamp) AS recency_days,
        COUNT(DISTINCT op.order_id) AS frequency_orders,
        SUM(op.payment_value) AS monetary
        FROM orders_with_payment op
        JOIN dim_customers c ON op.customer_id = c.customer_id
        GROUP BY c.customer_unique_id
),

rfm_scored AS (
    SELECT
        customer_unique_id,
        last_purchase_date,
        recency_days,
        frequency_orders,
        ROUND(monetary::NUMERIC, 2) AS monetary,

        NTILE(5) OVER(ORDER BY recency_days ASC) as recency_score,
        NTILE(5) OVER(ORDER BY frequency_orders DESC) as frecuency_score,
        NTILE(5) OVER(ORDER BY monetary DESC) as monetary_score
    FROM rfm_raw
),

rfm_segmented AS (
    SELECT
        customer_unique_id,
        last_purchase_date,
        recency_days,
        frequency_orders,
        monetary,
        recency_score,
        frequency_score,
        monetary_score,
        (recency_score + frequency_score + monetary_score) AS rfm_total,

        CASE
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4  THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 THEN 'Loyal customers'
            WHEN recency_score >= 4 AND frequency_score >= 2 THEN 'New customers'
            WHEN recency_score >= 2 AND frequency_score >= 3 THEN 'At risk'
            WHEN recency_score >= 1 AND frequency_score >= 1 THEN 'Lost'
            WHEN monetary_score >= 4 AND frequency_score >= 2 THEN 'Big Spenders'
            ELSER 'Potential Loyalist'
        END AS segment
    FROM rfm_scored
)

SELECT
    segment,
    COUNT(*) AS total_customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM rfm_segmented
GROUP BY segment
ORDER BY total_customers DESC;