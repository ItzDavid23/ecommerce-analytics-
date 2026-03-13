-- ============================================================
-- Análisis de Cohortes de clientes
--
-- Objetivo: Entender la retención de clientes agrupándolos
-- por el mes en que hicieron su primera compra (cohorte),
-- y rastreando cuántos volvieron a comprar en meses siguientes.
--
-- Concepto clave:
--  Cohorte: grupo de clientes que compraron por primera vez en el mismo mes.
--  Ejemplo: "cohorte de enero 2017".
--
-- Tablas usadas:
--  fact_orders → fechas de compra
--  dim_customers → ID único del cliente
--  dim_date → año y mes para agrupación

SET search_path TO ecommerce;

-- ============================================================
-- ANÁLISIS DE COHORTES COMPLETO
-- ============================================================

WITH

-- CTE 1: Primera compra de cada cliente (define su cohorte)
-- Usamos customer_unique_id porque customer_id puede repetirse
-- (un cliente puede tener múltiples customer_id en Olist)
first_purchase AS (
    SELECT
        c.customer_unique_id,
        MIN(o.order_purchase_timestamp) AS first_purchase_date,
        DATE_TRUNC('month', MIN(o.order_purchase_timestamp)) AS cohort_month,
    FROM fact_orders o JOIN dim_customers c ON o.customer_id = c.customer_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY customer_unique_id
),

-- CTE 2: Todas las compras de cada cliente con su cohorte asignado
all_purchases AS (
    SELECT
        c.customer_unique_id,
        fp.cohort_month,
        DATE_TRUNC('month', o.order_purchase_timestamp) AS purchase_month
        -- Convierte fechas a número de meses transcurridos desde la cohorte
        (
            EXTRACT(YEAR FROM o.order_purchase_timestamp) - EXTRACT(YEAR FROM fp.cohort_month)
        ) * 12
        +
        (
            EXTRACT(MONTH FROM o.order_purchase_timestamp)- EXTRACT(MONTH FROM fp.cohort_month)
        ) AS month_number
    FROM fact_orders o
    JOIN dim_customers c ON o.customer_id = c.customer_id
    JOIN fp_first_purchase fp ON c.customer_unique_id = fp.customer_unique_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
),

-- CTE 3: Tamaño de cada cohorte (clientes únicos en mes 0)
cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_unique_id) AS cohort_size
    FROM all_purchases
    WHERE month_number = 0
    GROUP BY cohort_month
)

-- CTE 4: Clientes activos por cohorte y mes relativo
cohort_activity AS (
    SELECT
        cohort_month,
        month_number,
        COUNT(DISTINCT customer_unique_id) AS active_customers
    FROM all_purchases
    GROUP BY cohort_month, month_number
)

-- ============================================================
-- RESULTADO FINAL: tabla de retención
-- Cada fila = un cohorte en un mes relativo
-- retention_rate = % de clientes que volvieron a comprar

SELECT
    TO_CHAR(ca.cohort_month, 'YYYY-MM') AS cohort,
    ca.month_number,
    cs.cohort_size AS initial_customers,
    ca.active_customers,
    ROUND(ca.active_customers * 100.0 / cs.cohort_size, 2) AS retention_rate
FROM cohort_activity ca JOIN cohort_sizes ON ca.cohort_month = cs.cohort_month
WHERE ca.month_number BETWEEN 0 AND 6 -- primeros 6 meses desde la primera compra
ORDER BY ca.cohort_mont, ca.month_number;

-- ============================================================
-- Tabla Pivote: retención en formato ancho
-- Más fácil de leer en un reporte o dashboard
-- Mes 0 siempre es 100% (primera compra)
-- ============================================================
WITH first_purchase AS (
    SELECT
        c.customer_unique_id,
        DATE_TRUNC('month', MIN(o.order_purchase_timestamp)) AS cohort_month
    FROM fact_orders o JOIN dim_customers c ON o.customer_id = c.customer_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY c.customer_unique_id
),

all_purchases AS(
    SELECT
        c.customer_unique_id,
        fp.cohort_month,
        (
            EXTRACT(YEAR FROM o.order_purchase_timestamp) - EXTRACT(YEAR FROM fp.cohort_month)
        ) * 12
        +
        (
            EXTRACT(MONTH FROM o.order_purchase_timestamp) - EXTRACT(MONTH FROM fp.cohort_month)
        ) AS month_number
    FROM fact_orders o
    JOIN dim_customers c ON o.customer_id = c.customer_id
    JOIN first_purchase fp ON c.customer_unique_id = fp.customer_unique_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
),

cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT fp.customer_unique_id) AS cohort_size
    FROM all_purchases
    WHERE month_number = 0
    GROUP BY cohort_month
),

cohort_activity AS(
    cohort_month,
    month_number,
    (COUNT(*) customer_unique_id) AS active_customers
    FROM all_purchases
    GROUP BY cohort_month, month_number
)

SELECT
    TO_CHAR(ca.cohort_month, 'YYYY-MM') AS cohort,
    cs.cohort_size

    -- Retención por mes (FILTER aplica COUNT solo cuando la condición es TRUE)
    ROUND(MAX(CASE WHEN ca.month_number = 0 THEN ca.active_customers END) * 100 / cs.cohort_size, 1) AS "Mes_0",
    ROUND(MAX(CASE WHEN ca.month_number = 1 THEN ca.active_customers END) * 100 / cs.cohort_size, 1) AS "Mes_1",
    ROUND(MAX(CASE WHEN ca.month_number = 2 THEN ca.active_customers END) * 100 / cs.cohort_size, 1) AS "Mes_2",
    ROUND(MAX(CASE WHEN ca.month_number = 3 THEN ca.active_customers END) * 100 / cs.cohort_size, 1) AS "Mes_3",
    ROUND(MAX(CASE WHEN ca.month_number = 4 THEN ca.active_customers END) * 100 / cs.cohort_size, 1) AS "Mes_4",
    ROUND(MAX(CASE WHEN ca.month_number = 5 THEN ca.active_customers END) * 100 / cs.cohort_size, 1) AS "Mes_5",
    ROUND(MAX(CASE WHEN ca.month_number = 6 THEN ca.active_customers END) * 100 / cs.cohort_size, 1) AS "Mes_6",
FROM cohort_activity ca JOIN cohort_size cs ON ca.cohort_month = cs.cohort_month
WHERE ca.month_number BETWEEN 0 AND 6
GROUP BY ca.cohort_month, cs.cohort_size
ORDER BY ca.cohort_month;

-- ============================================================
-- ANÁLISIS ADICIONAL: tiempo promedio entre primera y segunda compra
-- Responde: ¿Cuántos días tarda un cliente en volver?
-- ============================================================

WITH ordered_purchases AS(
    SELECT
        c.customer_unique_id,
        o.order_purchase_timestamp,
        -- LAG trae la fecha de la compra anterior del mismo cliente (si existe)
        LAG(o.order_purchase_timestamp) OVER(PARTITION BY c.customer_unique_id ORDER BY o.order_purchase_timestamp) AS previous_purchase_date,

        ROW_NUMBER() OVER(PARTITION BY c.customer_unique_id ORDER BY o.order_purchase_timestamp) AS purchase_number
    FROM fact_orders o
    JOIN dim_customers c ON o.customer_id = c.customer_id
    WHERE o.order_status NOT IN ('Canceled', 'unavailable')
)

SELECT
    AVG(order_purchase_timestamp - previous_purchase_date) AS avg_days_between_purchases,
    MIN(order_purchase_timestamp - previous_purchase_date) AS min_days_between_purchases,
    MAX(order_purchase_timestamp - previous_purchase_date) AS max_days_between_purchases,
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY (order_purchase_timestamp -previous_purchase_date)
    ) AS median_days_between_purchases
FROM ordered_purchases
WHERE purchase_number = 2 -- solo la segunda compra (intervalo 1->2)
AND previous_purchase_date IS NOT NULL; -- excluir clientes sin segunda compra

