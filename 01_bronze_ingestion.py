# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import col

raw_path = "/Volumes/ecommerce_project/default/raw_ecommerce/"
catalog_schema = "ecommerce_project.default"

print("Defining strict schemas...")

orders_schema = StructType(
    [
        StructField("order_id",StringType(),True),
        StructField("customer_id",StringType(),True),
        StructField("product_id",StringType(),True),
        StructField("quantity",IntegerType(),True),
        StructField("order_date",StringType(),True),
        StructField("satus",StringType(),True),
        StructField("payment_method", StringType(), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("shipped_date", StringType(), True),
        StructField("_corrupt_record", StringType(), True)
        
    ]
)

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("cost_price", DoubleType(), True),
    StructField("supplier_id", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

returns_schema = StructType([
    StructField("return_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("return_date", StringType(), True),
    StructField("reason_code", StringType(), True),
    StructField("refund_amount", DoubleType(), True),
    StructField("_corrupt_record", StringType(), True)
])

# COMMAND ----------

def ingest_to_bronze(table_name,file_name,schema):
    print(f"ingesting {table_name}...")

    # 1. Read with PERMISSIVE mode and specific corrupt record column

    df_raw  = spark.read.csv(
        f"{raw_path}{file_name}",
        header = True,
        schema = schema,
        mode = "PERMISSIVE",
        columnNameOfCorruptRecord='_corrupt_record'
    )
    # Pre-write Audit count
    rows_read = df_raw.count()

    # 2. Write to Delta (Unity Catalog handles the location and Hive Metastore registration)
    full_table_name = f"{catalog_schema}.bronze_{table_name}"
    
    write_format = "delta"
    write_mode = "overwrite"
    merge_schema = "true"
    (df_raw.write
     .format(write_format)
     .mode(write_mode)
     .option("mergeSchema",merge_schema)
     .saveAsTable(full_table_name)
     
     )
    
    # Post-write Audit count

    row_written = spark.read.table(full_table_name).count()

    # Check if any corrupt records were caught

    corrupt_count = spark.read.table(full_table_name).filter(col("_corrupt_record").isNotNull()).count()

    return{
        "table":table_name,
        "rows_read":rows_read,
        "rows_written":row_written,
        "corrupt_records_caught": corrupt_count,
        "status":"Success" if rows_read ==row_written else "WARNING"



    }

# COMMAND ----------

# Map our configurations

datasets = [
    {"name": "orders", "file": "orders.csv", "schema": orders_schema},
    {"name": "customers", "file": "customers.csv", "schema": customers_schema},
    {"name": "products", "file": "products.csv", "schema": products_schema},
    {"name": "returns", "file": "returns.csv", "schema": returns_schema}
]
audit_logs  = []
for data in datasets:
    audit_result = ingest_to_bronze(data["name"], data["file"], data["schema"])
    audit_logs.append(audit_result)

# Print the final Audit Dictionary
print("\n" + "="*50)
print("BRONZE INGESTION AUDIT LOG")
print("="*50)
for log in audit_logs:
    print(log)
print("="*50)