# Databricks notebook source
# Create a function to process daily order data
def process_daily_orders(date_str=None):
    """Process orders for a given date or the most recent data if date not specified"""
    if date_str is None:
        # Get the most recent date from the orders table
        latest_date = spark.sql("""
            SELECT DATE(MAX(order_purchase_timestamp)) as latest_date 
            FROM ecommerce_db.fact_orders
        """).collect()[0]['latest_date']
        date_str = latest_date.strftime("%Y-%m-%d")
    
    print(f"Processing orders for date: {date_str}")
    
    # Calculate daily metrics
    daily_metrics = spark.sql(f"""
    SELECT 
        DATE(order_purchase_timestamp) as order_date,
        COUNT(DISTINCT order_id) as total_orders,
        COUNT(DISTINCT customer_id) as unique_customers,
        SUM(price) as total_sales,
        AVG(price) as avg_order_value,
        SUM(freight_value) as total_shipping,
        COUNT(DISTINCT product_id) as unique_products
    FROM ecommerce_db.fact_orders
    WHERE DATE(order_purchase_timestamp) = '{date_str}'
    GROUP BY DATE(order_purchase_timestamp)
    """)
    
    # Save to a metrics table in our container (external table)
    metrics_path = "/mnt/presentation/daily_order_metrics"
    daily_metrics.write.format("delta").mode("append").save(metrics_path)
    
    # Create or replace the external table
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS ecommerce_db.daily_order_metrics
    USING DELTA
    LOCATION '{metrics_path}'
    """)
    
    print(f"Saved metrics for {date_str}")
    daily_metrics.display()
    
    return daily_metrics

# Test the function with the latest data
process_daily_orders()

