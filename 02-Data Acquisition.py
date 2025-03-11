# Databricks notebook source

#%pip install kagglehub

# Download the Brazilian E-commerce dataset using kagglehub
import kagglehub
import os
from zipfile import ZipFile

# Download latest version of the dataset
print("Downloading dataset from Kaggle...")
path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
print(f"Path to dataset files: {path}")

# List downloaded files
files = os.listdir(path)
print(f"Downloaded files: {files}")

# Copy files to our raw container in DBFS
for file in files:
    if file.endswith('.csv'):
        file_path = os.path.join(path, file)
        
        # Read the file content
        with open(file_path, 'rb') as f:
            file_content = f.read().decode('utf-8')
        
        # Save to raw container
        dbfs_path = f"/mnt/raw/{file}"
        dbutils.fs.put(dbfs_path, file_content, overwrite=True)
        print(f"Saved {file} to {dbfs_path}")

print("All files copied to raw container")