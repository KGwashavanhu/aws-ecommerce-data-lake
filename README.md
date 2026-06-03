# AWS Ecommerce Data Lake

An end-to-end data lake pipeline built on AWS, processing ecommerce transaction data through ingestion, transformation, and analytics layers.

## Architecture
Raw CSV Data → S3 (Raw Layer) → AWS Glue ETL → S3 (Curated Layer/Parquet) → Athena Queries
## Services Used

- **Amazon S3** — Data lake storage with Hive-style partitioning (year/month)
- **AWS Glue** — Crawler for schema discovery + ETL job converting CSV to Parquet
- **Amazon Athena** — Serverless SQL analytics on the curated data

## Dataset

Synthetic ecommerce orders dataset with 4,800 rows covering 2023–2024, including fields for product category, country, order status, quantity, and revenue.

## What This Project Demonstrates

- Designing a partitioned S3 data lake (raw → curated zones)
- Building a Glue ETL pipeline (CSV → Parquet conversion)
- Writing analytical SQL queries in Athena including partition pruning
- Automating Athena query execution with boto3

## Athena Queries

| Query | Description |
|---|---|
| Revenue by category | Total orders and revenue per product category |
| Monthly revenue trend | 24-month time series across 2023–2024 |
| Order status breakdown | Distribution of order statuses with percentages |
| Top countries (2024) | Partition-pruned query filtering to 2024 only |
| Top 5 products | Best sellers by quantity and revenue |

## Author

Kudakwashe Gwashavanhu — AWS Data Engineer  
[GitHub](https://github.com/KGwashavanhu)
