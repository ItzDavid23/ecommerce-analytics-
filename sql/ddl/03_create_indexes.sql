-- =====================================================
-- STAR SCHEMA - INDEXES
-- Database: ecommerce
-- Description:
-- This script creates indexes to improve query
-- performance on dimension and fact tables.
-- =====================================================

SET search_path TO ecommerce;

-- =====================================================
-- CUSTOMER INDEXES
-- =====================================================

CREATE INDEX idx_customers_unique_id
ON dim_customers(customer_unique_id);

CREATE INDEX idx_customers_city
ON dim_customers(customer_city);

CREATE INDEX idx_customer_state
ON dim_customers(customer_state);

-- =====================================================
-- PRODUCT INDEXES
-- =====================================================

CREATE INDEX idx_products_category
ON dim_products(product_category_name);

-- =====================================================
-- SELLER INDEXES
-- =====================================================

CREATE INDEX idx_seller_city
ON dim_sellers(seller_city);

CREATE INDEX idx_seller_state
ON dim_sellers(seller_state);

-- =====================================================
-- ORDER INDEXES
-- =====================================================

CREATE INDEX idx_order_customer
ON fact_orders(customer_id);

CREATE INDEX idx_purchase_date
ON fact_orders(order_purchase_date);

CREATE INDEX idx_order_status
ON fact_orders(order_status);

-- =====================================================
-- ORDER ITEMS INDEXES
-- =====================================================

CREATE INDEX idx_order_products
ON fact_order_items(product_id);

-- =====================================================
-- PAYMENTS INDEXES
-- =====================================================

CREATE INDEX idx_payment_orders
ON fact_order_payments(order_id);

-- =====================================================
-- REVIEWS INDEXES
-- =====================================================

CREATE INDEX idx_reviews_order
ON fact_order_reviews(order_id);

CREATE INDEX idx_reviews_score
ON fact_order_reviews(review_score);