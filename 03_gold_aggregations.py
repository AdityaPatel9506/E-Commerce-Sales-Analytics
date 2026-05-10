# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
catalog_schema = "ecommerce_project.default"
print("Loading Silver Enriched Data...")
# Load the master Silver table we created in Phase 3
df_silver = spark.read.table(f"{catalog_schema}.silver_enriched_orders")

# COMMAND ----------

print("Building Gold Table1: Daily Sales Summary...")

df_daily_sales = df_silver.groupBy("order_date","category").agg(
    round(sum("order_revenue"),2
           ).alias("total_revenue")
          ,count("order_id").alias("total_units_sold"),
          round(avg("order_revenue"),2).alias("avg_order__value"),
    round(sum(when(col("is_returned") ==True,1).otherwise(0))/count("order_id"), 4).alias("return_rate"))
display(df_daily_sales.limit(5))

# COMMAND ----------

# Make sure lit is imported at the top of your notebook if it isn't already!
from pyspark.sql.functions import lit 

print("Building Gold Table 2: Customer Analytics...")

# 1. Define Windows
window_cust_history = Window.partitionBy("customer_id").orderBy("order_date")


window_rank_spend = Window.partitionBy(lit(1)).orderBy(col("customer_lifetime_value").desc())

# 2. Get simulated today
simulated_today = df_silver.select(max("order_date")).collect()[0][0]

# 3. Calculate order-level lag
df_cust_lag = (
    df_silver
    .withColumn(
        "prev_order_date",
        lag("order_date").over(window_cust_history)
    )
    .withColumn(
        "days_between_orders",
        datediff(
            col("order_date"),
            col("prev_order_date")
        )
    )
)

# 4. Aggregate up to the Customer Level
df_customer_analytics = (
    df_cust_lag
    .groupBy(
        "customer_id",
        "name",
        "segment"
    )
    .agg(
        round(
            sum("order_revenue"), 2
        ).alias("customer_lifetime_value"),
        max("order_date").alias("last_order_date"),
        round(
            avg("days_between_orders"), 1
        ).alias("avg_days_between_purchases")
    )
)

# 5. Apply Rankings and Churn Signal
df_customer_final = (
    df_customer_analytics
    .withColumn(
        "purchase_rank", rank().over(window_rank_spend)
    )
    .withColumn(
        "days_since_last_order",
        datediff(
            lit(simulated_today), 
            col("last_order_date")
        )
    )
    .withColumn(
        "churn_signal",
        when(
            col("days_since_last_order") >= 90,
            "At Risk"
        ).otherwise("Active")
    )
)

display(df_customer_final.orderBy("purchase_rank").limit(5))

# COMMAND ----------

print("Building Gold Table 3: Product Performance...")

# 1. Aggregate to the Product-Month Level first

df_prod_base = df_silver.groupBy(
    "order_year","order_month","category","product_id","product_name"
).agg(
    round(sum("order_revenue"),2).alias("product_revenue")
)

# 2. Define Windows (Safely partitioned!)

window_cat_month = Window.partitionBy("order_year","order_month","category")
window_prod_hist = Window.partitionBy("product_id").orderBy("order_year","order_month")

# 3. Apply Advanced Metrics

df_product_performance = (
    df_prod_base
    .withColumn("category_monthly_revenue",sum("product_revenue").over(window_cat_month))
    .withColumn("percent_of_total_revenue",round((col("product_revenue")/col("category_monthly_revenue"))*100,2))
    .withColumn("prev_month_revenue",lag("product_revenue").over(window_prod_hist))
    .withColumn("mom_revenue_change",round(col("product_revenue")-col("prev_month_revenue"),2))
    .withColumn("category_rank",dense_rank().over(window_cat_month.orderBy(col("product_revenue").desc())))
)
# Filter to show only the Top 3 products per category per month
df_product_top_N = df_product_performance.filter(col("category_rank") <= 3)

# Display a preview
display(df_product_top_N.orderBy("order_year", "order_month", "category", "category_rank").limit(10))

# COMMAND ----------

print("Persisting Gold Tables to Unity Catalog...")

# 1. Write Daily Sales
df_daily_sales.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_schema}.gold_daily_sales")

# 2. Write Customer Analytics
df_customer_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_schema}.gold_customer_analytics")

# 3. Write Product Performance
df_product_top_N.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_schema}.gold_product_performance")

print("Phase 4 Complete! The Medallion Architecture is fully implemented.")