# Databricks notebook source
# MAGIC %md
# MAGIC # Environment Setup

# COMMAND ----------

raw_path = '/Volumes/ecommerce_project/default/raw_ecommerce'

# Verify the files exist in the Volume
try:
    files = dbutils.fs.ls(raw_path)
    files:dbutils.fs.ls(raw_path)
    display(files)
except Exception as e:
    print(f"error finding path {e} ")

# COMMAND ----------

# Cell 2: Quick test to read the Bronze (Raw) Orders data
print("Previewing Raw Orders Data...")

# Hardcoding the exact path to guarantee no concatenation errors
exact_file_path = "/Volumes/ecommerce_project/default/raw_ecommerce/orders.csv"

df_orders_raw = spark.read.format("csv") \
    .option("header", "true") \
    .load(exact_file_path)

display(df_orders_raw.limit(5))