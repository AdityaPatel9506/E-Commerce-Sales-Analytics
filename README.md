# E-Commerce-Sales-Analytics
End-to-End PySpark Pipeline with Advanced Analytics

# 🚀 E-Commerce Data Engineering Pipeline (PySpark + Databricks)

## 📌 Project Overview

Real-world e-commerce platforms generate massive volumes of transactional data across orders, customers, products, and logistics. The challenge is not just storing this data, but building a scalable distributed processing pipeline that ensures data quality, performance optimization, and actionable business insights.

This project simulates a real production-grade data engineering environment using **PySpark on Databricks Community Edition**, following the **Medallion (Lakehouse) Architecture**. It demonstrates an end-to-end pipeline from raw ingestion to analytics-ready datasets.

---

## 🎯 Project Objectives

- Design a **multi-layer data architecture (Bronze, Silver, Gold)** using Lakehouse principles
- Ingest and validate raw CSV/JSON datasets with schema enforcement and null handling
- Perform complex transformations using **PySpark DataFrame API, Spark SQL, and Window functions**
- Optimize performance using **partitioning, caching, and broadcast joins**
- Build business-ready analytical datasets for reporting and dashboards
- Derive key business metrics such as:
  - Customer Lifetime Value (CLV)
  - Cohort Retention
  - Product ABC Analysis
  - Funnel Conversion Metrics
- Analyze and optimize Spark execution using `explain()` and Spark UI

---

## 🧠 Why This Project Matters

This project is designed to reflect real-world responsibilities of a **PySpark / Data Engineer**:

- Handling large-scale distributed data processing
- Making trade-offs between different join strategies (broadcast vs sort-merge)
- Writing idempotent and production-ready pipelines
- Designing scalable data models for analytics
- Supporting BI teams with clean, aggregated datasets

---

## 🏗️ Architecture (Medallion / Lakehouse)

The pipeline follows the Medallion Architecture used in modern data platforms like Databricks.

### 🔵 Bronze Layer (Raw Data)
- Raw ingestion of CSV/JSON files
- No transformations applied
- Append-only storage
- Schema inference enabled with basic validation

### 🟡 Silver Layer (Cleansed Data)
- Data type casting and validation
- Deduplication and null handling
- Joins across domains (orders, customers, products, returns)
- Derived business columns (e.g., order value, shipping duration)

### 🟢 Gold Layer (Business Aggregates)
- Pre-aggregated analytics-ready tables
- Optimized for BI tools and dashboards
- Partitioned for query performance
- Includes KPIs, cohort tables, and product/customer insights

---

## 📊 Dataset Overview

The project uses four synthetic but realistic datasets:

### 📦 Orders (`orders.csv`)
- `order_id`, `customer_id`, `product_id`, `quantity`
- `order_date`, `shipped_date`, `status`
- `payment_method`, `discount_pct`
- ~100K records

---

### 👤 Customers (`customers.csv`)
- `customer_id`, `name`, `email`
- `city`, `state`, `country`
- `signup_date`, `segment` (B2B / B2C)
- ~20K records

---

### 🛍️ Products (`products.csv`)
- `product_id`, `product_name`
- `category`, `subcategory`
- `unit_price`, `cost_price`
- `supplier_id`
- ~2K records

---

### 🔁 Returns (`returns.csv`)
- `return_id`, `order_id`
- `return_date`, `reason_code`
- `refund_amount`
- ~8K records

---

## ⚙️ Tech Stack

- **Databricks Community Edition (DBR 13.x / Spark 3.4+)**
- **PySpark (DataFrame API + Spark SQL)**
- **Delta Lake (Lakehouse storage format)**
- **Python 3.10**
- **DBFS (Databricks File System)**
- Notebooks using `%python`, `%sql`, `%md`

---

## 📈 Key Features Implemented

- 🔄 End-to-end ETL pipeline (Bronze → Silver → Gold)
- ⚡ Optimized Spark transformations (broadcast joins, caching, partitioning)
- 📊 Cohort analysis and retention modeling
- 📦 ABC product classification (Pareto analysis)
- 👥 Customer segmentation using NTILE quartiles
- 💳 Payment method distribution analysis
- 🔁 Funnel conversion metrics (Order → Ship → Return)
- 📉 Performance tuning using Spark explain plans

---

## 🧪 Example Business Insights

- Top 20% of products contribute ~80% of revenue (Pareto principle)
- Clear segmentation of customers into Platinum, Gold, Silver, Bronze tiers
- Significant variation in return rates across product categories
- Strong correlation between payment method and conversion rates

---

## 🚀 How to Run

1. Upload datasets to **DBFS**
2. Open Databricks Community Edition
3. Run notebooks in the following order:
   - Bronze ingestion notebook
   - Silver transformation notebook
   - Gold aggregation notebook
4. View outputs using `display()` or Spark SQL queries

---

## 📌 Future Enhancements

- Integrate Delta Live Tables (DLT)
- Add Airflow orchestration
- Build Power BI / Tableau dashboards
- Implement real-time streaming ingestion
- Add unit testing for PySpark transformations

---

## 👨‍💻 Author

Built as a **hands-on Data Engineering project** to simulate production-level PySpark workflows and demonstrate end-to-end Lakehouse architecture implementation.

---

## ⭐ If you like this project

Feel free to star ⭐ the repository and explore the pipeline design!
