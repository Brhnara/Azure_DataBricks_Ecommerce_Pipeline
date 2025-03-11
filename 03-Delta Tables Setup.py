# Databricks notebook source
# List the CSV files in the raw container
files = dbutils.fs.ls("/mnt/raw/")
csv_files = [f.name for f in files if f.name.endswith('.csv')]
print(f"CSV files available: {csv_files}")

# Function to create Delta tables from CSV files
def create_delta_table(file_name):
    print(f"Processing {file_name}...")
    
    # Extract table name from file name (remove extension)
    table_name = file_name.replace(".csv", "")
    
    # Read CSV file with header
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"/mnt/raw/{file_name}")
    
    # Print schema and sample data
    print(f"Schema for {table_name}:")
    df.printSchema()
    df.show(5)
    
    # Save as Delta format to the processed container
    delta_path = f"/mnt/processed/{table_name}"
    df.write.format("delta").mode("overwrite").save(delta_path)
    print(f"Saved Delta table to: {delta_path}")
    
    # Create database if needed
    spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_db")
    
    # Create external table pointing to the Delta files
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS ecommerce_db.{table_name}
    USING DELTA
    LOCATION '{delta_path}'
    """)
    print(f"Created Delta table: ecommerce_db.{table_name}")
    
    return df

# Create tables for each CSV file
for file_name in csv_files:
    create_delta_table(file_name)

print("All Delta tables created successfully!")