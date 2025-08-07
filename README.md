# Databricks Data Ingestor

## Overview

This codebase enables **seamless ingestion of CSV files into Databricks** using **Delta Lake tables**. It ensures robust data transfers with schema enforcement, batch processing, fault tolerance, and parallel processing.

## Features

- **Automated CSV Ingestion**: Reads and loads CSV files into a Databricks Delta table.  
- **Schema Enforcement**: Creates tables dynamically while ensuring data integrity.  
- **Efficient Batch Processing**: Processes large datasets in chunks with parallel execution.  
- **Multiprocessing Support**: Uses multiple processes to speed up ingestion.  
- **Retry Mechanism**: Handles transient failures (HTTP 429, 503) with automatic retries.  
- **SQL Injection Prevention**: Escapes values to prevent SQL injection attacks.  
- **Error Logging**: Captures detailed error messages for debugging.  

## Prerequisites

Ensure the following dependencies are installed:

- **Python 3.7+**
- **Databricks SQL Connector** (`pip install databricks-sql-connector`)
- **Pandas** (`pip install pandas`)

## Configuration

Update the placeholders in the script with your **Databricks credentials** and **schema details**:

```python
connection_details = {
    "server_hostname": "<DATABRICKS_HOSTNAME>",
    "http_path": "<DATABRICKS_HTTP_PATH>",
    "access_token": "<DATABRICKS_ACCESS_TOKEN>"
}

file_path = "<LOCAL_FILE_PATH>"
schema = "<SCHEMA_NAME>"
table_name = "<TABLE_NAME>"
```

## Usage

1. **Import Required Libraries**
```python
import pandas as pd
from databricks import sql
from multiprocessing import Pool
```
2. **Run Ingestion**
```python
upload_file_to_databricks(df, table_name, schema="your_schema", chunk_size=5000, num_processes=10)
```

## How It Works

### 1. Table Creation (If Not Exists)
The script automatically creates a Delta table if it doesn’t exist:
```sql
CREATE TABLE IF NOT EXISTS <SCHEMA_NAME>.<TABLE_NAME>
USING delta
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5'
)
AS SELECT CAST(NULL AS STRING) AS column1 WHERE 1=0;
```

### 2. Parallel Batch Data Insertion
Data is inserted in chunks with multiprocessing for efficiency:
```python
with Pool(processes=num_processes) as pool:
    results = [pool.apply_async(process_chunk, args=(i, chunk, schema, table_name, connection_details)) for i, chunk in enumerate(chunks)]
    pool.close()
    pool.join()
```

### 3. Retry Logic & Error Handling
- **Retry logic** prevents failures due to transient errors.
- **SQL Injection Prevention** ensures data security.
- **Multiprocessing** speeds up large file ingestion.

## Best Practices

**Use Delta Lake**: Ensures ACID compliance and schema evolution.  
**Optimize Batch Size**: Adjust `chunk_size` based on system resources.  
**Monitor Parallelism**: Tune `num_processes` for best performance.  
**Validate Data Before Ingestion**: Prevents schema mismatches.  

---
