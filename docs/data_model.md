# Data Model Documentation

## Overview

This project uses a star schema model designed for analytical queries
on ecommerce data.

The model supports analysis of:
- Customer behavior
- Product performance
- Revenue trends
- Customer satisfaction

---

## Schema Design

The schema follows a star schema structure with:

Fact tables:
- fact_orders
- fact_order_items
- fact_order_payments
- fact_order_reviews

Dimension tables:
- dim_customers
- dim_products
- dim_sellers
- dim_date

---

## Fact Tables

### fact_orders
Stores order-level information.

Grain:
One row per order.

---

### fact_order_items
Stores product-level order information.

Grain:
One row per product per order.

---

### fact_order_payments
Stores payment transactions.

Grain:
One row per payment event.

---

### fact_order_reviews
Stores customer reviews.

Grain:
One row per review.

---

## Dimension Tables

### dim_customers
Stores customer attributes.

### dim_products
Stores product attributes.

### dim_sellers
Stores seller attributes.

### dim_date
Stores calendar attributes.

---

## Index Strategy

Indexes were created on:
- Foreign keys
- Filter columns
- Join columns

To improve performance on analytical queries.

---

## Why Star Schema

The star schema was chosen because:

- Simplifies queries
- Improves aggregation performance
- Optimized for BI tools
- Easier to understand