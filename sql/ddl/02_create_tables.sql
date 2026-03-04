-- =====================================================
-- STAR SCHEMA - TABLE CREATION
-- Database: ecommerce
-- Description:
-- This script creates all dimension and fact tables
-- for the ecommerce star schema model.
-- =====================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS ecommerce;
SET search_path TO ecommerce;

-- =====================================================
-- DIMENSIONS
-- =====================================================

-- Customers dimension
-- Stores customer descriptive attributes
CREATE TABLE dim_customers(
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_city VARCHAR(100),
    customer_state CHAR(2)
);

-- Products dimension
-- Stores product descriptive attributes
CREATE TABLE dim_products(
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_length SMALLINT,
    product_description_length SMALLINT,
    product_photos_qty SMALLINT,
    product_weight_g INTEGER,
    product_length_cm SMALLINT,
    product_height_cm SMALLINT,
    product_width_cm SMALLINT
);

-- Sellers dimension
-- Stores seller descriptive attributes
CREATE TABLE dim_sellers(
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(30),
    seller_state CHAR(2)
);

-- Date dimension
-- Used for time-based analysis
CREATE TABLE dim_date(
    date_id DATE PRIMARY KEY,
    year SMALLINT,
    quarter SMALLINT,
    month SMALLINT,
    week SMALLINT,
    day SMALLINT,
    is_weekend BOOLEAN
);

-- =====================================================
-- FACT TABLES
-- =====================================================

-- Orders fact table
-- Stores order-level information
CREATE TABLE fact_orders(
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_date DATE,
    order_approved_date DATE,
    order_delivered_date DATE,
    order_estimated_date DATE
);

-- Order items fact table
-- Stores product-level order information
CREATE TABLE fact_order_items(
    order_id VARCHAR(50),
    order_item_id SMALLINT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    price NUMERIC(10, 2),
    freight_value NUMERIC(10, 2),
    PRIMARY KEY(order_id, order_item_id)
);

-- Payments fact table
-- Stores payment transactions
CREATE TABLE fact_order_payments(
    order_id VARCHAR(50),
    payment_sequential SMALLINT,
    payment_type VARCHAR(30),
    payment_installments SMALLINT,
    payment_value NUMERIC(10, 2),
    PRIMARY KEY(order_id, payment_sequential)
);

-- Reviews fact table
-- Stores customer review information
CREATE TABLE fact_order_reviews(
    review_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    review_score SMALLINT,
    review_creation_date DATE,
    review_answer_date DATE
);