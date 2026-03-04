-- =====================================================
-- STAR SCHEMA - CONSTRAINTS
-- Database: ecommerce
-- Description:
-- This script adds foreign keys and constraints
-- between dimension and fact tables.
-- =====================================================

SET search_path TO ecommerce;

-- =====================================================
-- FACT ORDERS CONSTRAINTS
-- =====================================================

ALTER TABLE fact_orders
ADD CONSTRAINT fk_orders_customer
FOREIGN KEY (customer_id)
REFERENCES dim_customers(customer_id);

ALTER TABLE fact_orders
ADD CONSTRAINT fk_orders_date
FOREIGN KEY (order_purchase_date)
REFERENCES dim_date(date_id);

-- =====================================================
-- FACT ORDER ITEMS CONSTRAINTS
-- =====================================================

ALTER TABLE fact_order_items
ADD CONSTRAINT fk_items_orders
FOREIGN KEY (order_id)
REFERENCES fact_orders(order_id);

ALTER TABLE fact_order_items
ADD CONSTRAINT fk_items_products
FOREIGN KEY (product_id)
REFERENCES dim_products(product_id);

ALTER TABLE fact_order_items
ADD CONSTRAINT fk_items_sellers
FOREIGN KEY (seller_id)
REFERENCES dim_sellers(seller_id);

-- =====================================================
-- FACT PAYMENTS CONSTRAINTS
-- =====================================================

ALTER TABLE fact_order_payments
ADD CONSTRAINT fk_payments_orders
FOREIGN KEY (order_id)
REFERENCES fact_orders(order_id);

-- =====================================================
-- FACT REVIEWS CONSTRAINTS
-- =====================================================

ALTER TABLE fact_order_reviews
ADD CONSTRAINT fk_reviews_orders
FOREIGN KEY (order_id)
REFERENCES fact_orders(order_id);

ALTER TABLE fact_order_reviews
ADD CONSTRAINT chk_review_score
CHECK (review_score BETWEEN 1 and 5);