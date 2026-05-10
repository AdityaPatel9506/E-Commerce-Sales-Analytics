# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

catalog_schema = "ecommerce_project.default"

print("Loading Silver Enriched Data...")
df_silver = spark.read.table(f"{catalog_schema}.silver_enriched_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC # Cohort Analysis (Retention Matrix)
# MAGIC This calculates the classic SaaS/E-commerce cohort matrix. We find the customer's "birth month" (first purchase), calculate the distance in months to their subsequent purchases, and pivot the results.

# COMMAND ----------



# COMMAND ----------

rint("Building Calendar-Based Cohort Matrix (Newest to Oldest)...")

# 1. Find the first purchase month for each customer (their 'Cohort')
df_cohorts = (
    df_silver
    .groupBy("customer_id")
    .agg(
        trunc(
            min("order_date"),
            "month"
        ).alias("cohort_month")
    )
)

# 2. Join back to orders to attach the cohort month to every transaction
df_cohort_orders = (
    df_silver
    .join(
        df_cohorts,
        on="customer_id",
        how="inner"
    )
    .withColumn(
        "calendar_month",  
        date_format(col("order_date"), "yyyy-MM")
    )
)

# 3. Pivot using the actual calendar month instead of the relative index
# 3a. Dynamically grab all the unique months and sort them DESCENDING (Newest first)
month_list_rows = (
    df_cohort_orders
    .select("calendar_month")
    .distinct()
    .orderBy(col("calendar_month").desc())
    .collect()
)

# Extract just the string values into a standard Python list
months_descending = [row["calendar_month"] for row in month_list_rows]

# 3b. Pivot passing the explicit sorted list
df_calendar_matrix  = (
    df_cohort_orders
    .groupBy("cohort_month")
    .pivot("calendar_month", months_descending)  
    .agg(
        countDistinct("customer_id")
    )
    .orderBy(col("cohort_month").desc()) 
)

display(df_calendar_matrix)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rolling Metrics (rangeBetween vs rowsBetween)
# MAGIC
# MAGIC Concept Focus: rowsBetween(-6, 0) blindly grabs the 6 physical rows before the current row. If your shop had zero sales on Tuesday, the row is missing, and Spark pulls in last week's data by mistake. rangeBetween looks at the actual logical value of the date. It correctly handles missing days!

# COMMAND ----------

print("Calculating Rolling 7-Day & 30-Day Revenue...")
# 1. Aggregate to Daily Revenue first
df_daily = df_silver.groupBy("order_date").agg(sum("order_revenue").alias("daily_revenue"))

# 2. Convert date to a UNIX timestamp (seconds) so rangeBetween can do the math

df_daily  = df_daily.withColumn("date_unix",unix_timestamp(col("order_date").cast("timestamp")))

# 3. Define the Windows (86,400 seconds in a day)

days = lambda d:d*86400

# rangeBetween looks back X days, ending at 0 (the current row)

window_7d = Window.orderBy("date_unix").rangeBetween(-days(6),0)
window_30d = Window.orderBy("date_unix").rangeBetween(-days(29),0)

#  Step 4: Calculate rolling revenue metrics
df_rolling_metrics = (
    df_daily
    # 7-day rolling revenue
    .withColumn(
        "rolling_7d_rev",
        round(
            sum("daily_revenue").over(window_7d),2
        )
    )
    # 30-day rolling revenue
    .withColumn(
        "rolling_30d_rev",
        round(
            sum("daily_revenue").over(window_30d),2
        )
    )
    # Remove helper column
        .drop("date_unix")

        # Sort by date
        .orderBy("order_date")

)

display(df_rolling_metrics.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # ABC Analysis & Percentile Segmentation
# MAGIC This calculates total revenue, builds a cumulative sum down the list, and categorizes products and customers dynamically.
# MAGIC

# COMMAND ----------

print("Executing ABC Analysis and Percentile Segmentation...")

# ==========================================
# 1. ABC Product Analysis
# ==========================================
# Base product revenue

df_prod_rev = (
    df_silver.groupBy("product_id","product_name").agg(round(sum("order_revenue"),2).alias("total_revenue"))

)

# Windows for cumulative sum and global sum

window_abc = Window.orderBy(col("total_revenue").desc())
window_global = Window.partitionBy(lit(1))

df_abc_products = (
    df_prod_rev
    .withColumn(
        "cum_revenue",
        sum("total_revenue").over(window_abc)
    )
    # Compute total global revenue
    .withColumn(
        "global_revenue",
        sum("total_revenue").over(window_global)
    )
    # Convert to cumulative percentage
    .withColumn(
        "cum_pct",
        col("cum_revenue")/col("global_revenue")
    )
    
    # Assign ABC classification
    .withColumn(
        "abc_class",
        when(col("cum_pct") <= 0.80,"A")
        .when(col("cum_pct") <=0.95,"B")
        .otherwise("C")
    )
    # Step 5: Drop intermediate columns
    .drop("global_revenue", "cum_revenue")

)

# ==========================================
# 2. Customer Loyalty Quartiles using ntile(4)
# ==========================================

df_cust_rev = df_silver.groupBy("customer_id","name").agg(sum("order_revenue").alias("clv"))
window_ntile = Window.orderBy(col("clv").desc())

df_customer_tiers = (
    df_cust_rev
    .withColumn(
        "quartile",
        ntile(4).over(window_ntile)
    )
    
    # Map quartiles to loyalty tiers

    .withColumn(
        "loyalty_tier",
        when(col("quartile") == 1,"Platinum")
        .when(col("quartile") ==2 ,"Gold")
        .when(col("quartile") ==3 ,"Silver")
        .otherwise("Bronze")
    )
    # Step 3: Remove helper column
    .drop("quartile")
)



display(df_abc_products.sample(False, 0.01).limit(20))
display(df_customer_tiers.filter(col("loyalty_tier") == "Platinum").limit(5))
display(df_customer_tiers.filter(col("loyalty_tier") == "Gold").limit(5))
display(df_customer_tiers.filter(col("loyalty_tier") == "Silver").limit(5))
display(df_customer_tiers.filter(col("loyalty_tier") == "Bronze").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Funnel Analysis & Payment Breakdown
# MAGIC
# MAGIC Instead of a DataFrame, we will output these high-level metrics directly as a Python dictionary for easy dashboard reading.

# COMMAND ----------

print("="*50)
print("FUNNEL ANALYSIS & PAYMENT METRICS")
print("="*50)

# 1. Total Base
total_orders = df_silver.count()

# 2. Funnel Stages
shipped_orders = df_silver.filter(col("satus").isin("Shipped","Delivered")).count()
returned_orders = df_silver.filter(col("is_returned") == True).count()

print("shipped order count ",shipped_orders)
print("returned order count ",returned_orders)
# 3. Conversion Rates
order_to_ship_pcent = (shipped_orders / total_orders) * 100
ship_to_return_pcent = (returned_orders / total_orders) * 100

print(f"Total Orders Received:  {total_orders:,}")
print(f"Orders Successfully Shipped: {shipped_orders:,} ({order_to_ship_pcent}%)")
print(f"Shipped Orders Returned:     {returned_orders:,} ({ship_to_return_pcent}%)\n")

# 4. Payment Method Breakdown
print("Payment Method Breakdown:")

df_payments = (
    df_silver
    .groupBy("payment_method")
    .agg(
        count("order_id").alias("count")
    )
        
    # Step 2: Compute percentage of total orders

    .withColumn(
        "pcent_of_total",
        round(
            (col("count")/total_orders)*100,2
        )
    )
    
    # Sort by highest share
    .orderBy(
        col("pcent_of_total").desc()
    )
)

df_payments.show()