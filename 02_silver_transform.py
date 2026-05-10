# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

catalog_schema = "ecommerce_project.default"

# Load Bronze Tables
df_bronze_orders = spark.read.table(f"{catalog_schema}.bronze_orders")
df_bronze_customers = spark.read.table(f"{catalog_schema}.bronze_customers")
df_bronze_products = spark.read.table(f"{catalog_schema}.bronze_products")
df_bronze_returns = spark.read.table(f"{catalog_schema}.bronze_returns")
                              

# COMMAND ----------

print("Applying Schema validation and NULL handeling")

df_orders_cast = (
    df_bronze_orders
    .withColumn(
        "order_date",
        to_date(col("order_date"),'yyyy-MM-dd')
    )
    .withColumn(
        "shipped_date",
        to_date(col("shipped_date"),'yyyy-MM-dd')
    )
)
# Drop non-nullable keys
df_orders_valid = df_orders_cast.dropna(subset=['order_id','customer_id'])

# Calculate median for discount_pct using approxQuantile
# [0.5] means 50th percentile (median), 0.01 is the relative error tolerance for performance

median_discount = df_orders_valid.approxQuantile("discount_pct",[0.5],0.01)[0]

df_orders_filled = df_orders_valid.fillna({"discount_pct":median_discount})

print(f"Calculated Median Discount to fill NULLs: {median_discount}")

# COMMAND ----------

print("Performing Window Deduplication...")
window_spec = Window.partitionBy("order_id").orderBy(col("order_date").desc())
df_orders_dedup = (
    df_orders_filled
    .withColumn(
        'rn',
        row_number().over(window_spec)
    )
    .filter(
        col("rn") ==1
    )
    .drop("rn")
)



# COMMAND ----------

print("Calculating Derived Columns and Executing Joins...")

# Prepare dimensions (dropping the quarantine column from Bronze)

df_returns_clean = df_bronze_returns.select('order_id').withColumn("is_returned",lit(True)).distinct()
df_products_clean = df_bronze_products.drop("_corrupt_record")
df_customers_clean = df_bronze_customers.drop("_corrupt_record")

# 1. Join Orders to Returns (Left Join to get is_returned flag)

df_step1 = df_orders_dedup.join(df_returns_clean,on='order_id',how="left").fillna({"is_returned":False})

# 2. Join to Products (Broadcast Join - sending small table to all worker nodes)
df_step2 = df_step1.join(broadcast(df_products_clean),on ="product_id", how = 'inner')

# 3. Join to Customers (SortMergeJoin Hint - preventing out-of-memory errors on massive tables)
df_joined_final = df_step2.join(df_customers_clean.hint("merge"),on="customer_id",how="inner")

df_silver_enriched = (
    df_joined_final
    .withColumn(
        "order_revenue",
        col("quantity")*col("unit_price") *(1-col("discount_pct"))
    )
    .withColumn(
        "days_to_ship",
        datediff(
            col("shipped_date"),
            col("order_date")
        )
    )
    .withColumn(
        "order_year",
        year(col("order_date"))
    )
    .withColumn(
        "order_month",
        month(col("order_date"))
    )
    .drop("_corrupt_record")
)

# COMMAND ----------

# Print the physical execution plan
print("="*50)
print("PHYSICAL EXECUTION PLAN")
print("="*50)
df_silver_enriched.explain(True)

# COMMAND ----------

print("Writing partitioned Delta Table to Silver...")

df_silver_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("order_year", "order_month") \
    .saveAsTable(f"{catalog_schema}.silver_enriched_orders")

print(" Silver layer successfully constructed!")