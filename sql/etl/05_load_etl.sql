-- ================================================
-- 05_load_etl.sql
-- Validaciones y configuraciones post-carga ETL
-- ================================================

-- ================================================
-- 1. VERIFICAR CONTEOS POST-CARGA
-- Ejecutar esto después del ETL para confirmar que
-- los datos llegaron correctamente
-- ================================================

SELECT 'dim_customers' AS tabla, COUNT(*) AS filas FROM dim_customers
UNION ALL

SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL

SELECT 'dim_sellers', COUNT(*) FROM dim_sellers
UNION ALL

SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL

SELECT 'fact_orders', COUNT(*) FROM fact_orders
UNION ALL

SELECT 'fact_order_items', COUNT(*) FROM fact_order_items
UNION ALL

SELECT 'fact_order_payments', COUNT(*) FROM dim_order_payments
UNION ALL

SELECT 'fact_order_reviews', COUNT(*) FROM fact_order_reviews;
UNION ALL


-- ================================================
-- 2. VERIFICAR INTEGRIDAD REFERENCIAL MANUAL
-- Detecta items, pagos, o reviews que apunten a órdenes inexistentes
-- ================================================

SELECT COUNT(*) AS items_sin_orden
FROM fact_order_items i
WHERE NOT EXISTS(
    SELECT 1 FROM fact_orders o
    WHERE o.order_id = i.order_id
);

SELECT COUNT(*) AS pagos_sin_orden
FROM fact_order_payments p
WHERE NOT EXISTS(
    SELECT 1 FROM fact_orders o
    WHERE o_order_id = p.order_id
);

SELECT COUNT(*) AS reviews_sin_orden
FROM fact_order_reviews r
WHERE NOT EXISTS(
    SELECT 1 FROM fact_orders o
    WHERE o.order_id = r.order_id
)

-- ================================================
-- 3. VERIFICAR NULOS EN COLUMNAS CRÍTICAS
-- ================================================

SELECT
    COUNT(*) FILTER (WHERE customer_id IS NULL) AS null_customer_id,
    COUNT(*) FILTER (WHERE order_status IS NULL) AS null_status,
    COUNT(*) FILTER (WHERE order_purchase_date IS NULL) AS null_purchase_date
FROM fact_orders;

-- ================================================
-- 4. RANGO DE FECHAS CARGADAS
-- Confirma que los datos cubren el período esperado
-- ================================================

SELECT
    MIN(order_purchase_date) AS primera_order,
    MAX(order_purchase_date) AS ultima_order,
    COUNT(DISTINCT order_purchase_date) AS dias_distintos
FROM fact_orders;

-- ================================================
-- 5. DISTRIBUCIÓN DE ESTADOS DE ÓRDENES
-- Si algo salió mal en la limpieza, aquí aparecerá
-- un estado inesperado
-- ================================================

SELECT order_status, COUNT(*) AS total
FROM fact_orders
GROUP BY order_status
ORDER BY total desc