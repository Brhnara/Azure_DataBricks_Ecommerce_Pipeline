# Databricks notebook source
# List all tables in our database
spark.sql("SHOW TABLES IN ecommerce_db").display()

# Let's examine each table's schema and sample data
tables = spark.sql("SHOW TABLES IN ecommerce_db").select("tableName").rdd.flatMap(lambda x: x).collect()

for table in tables:
    print(f"\n\n=== Table: {table} ===")
    # Show table schema
    print("\nSchema:")
    spark.sql(f"DESCRIBE TABLE ecommerce_db.{table}").display()
    
    # Show sample data
    print("\nSample Data:")
    spark.sql(f"SELECT * FROM ecommerce_db.{table} LIMIT 10").display()
    
    # Row count
    count = spark.sql(f"SELECT COUNT(*) as count FROM ecommerce_db.{table}").collect()[0]['count']
    print(f"\nTotal rows: {count}")

# COMMAND ----------

# Create a fact table for orders with related dimensions - as an external table
# First, save the data to our processed container
spark.sql("""
SELECT 
    o.order_id,
    o.customer_id,
    oi.seller_id,
    oi.product_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    oi.price,
    oi.freight_value,
    oi.shipping_limit_date
FROM ecommerce_db.olist_orders_dataset o
JOIN ecommerce_db.olist_order_items_dataset oi ON o.order_id = oi.order_id
""").write.format("delta").mode("overwrite").save("/mnt/processed/fact_orders")

# Then create an external table pointing to this location
spark.sql("""
CREATE TABLE IF NOT EXISTS ecommerce_db.fact_orders
USING DELTA
LOCATION '/mnt/processed/fact_orders'
""")