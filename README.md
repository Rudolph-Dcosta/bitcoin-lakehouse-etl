# bitcoin-lakehouse-etl
Bitcoin Data Lakehouse End-to-End Medallion Architecture

Overview
This project implements a cloud-native Data Lakehouse using the Medallion Architecture. It automates the ingestion of real-time Bitcoin price data from the CoinGecko REST API into a Google Cloud Storage (GCS) Data Lake, followed by multi-stage transformations using PySpark and Delta Lake on Databricks.

Component,Technology
Cloud Provider,Google Cloud Platform (GCP)
Data Lake,Google Cloud Storage (GCS)
Compute Engine,Databricks (Apache Spark 3.5+)
Data Governance,Unity Catalog
Table Format,Delta Lake (ACID Transactions)
Orchestration,Databricks Workflows
Languages,"Python (PySpark), SQL"

Architecture Layers
1. Ingestion (Python & GCS)
A Python-based automation script fetches Bitcoin prices and metadata. Data is stored as partitioned JSON in GCS (raw/YYYY/MM/DD/) to optimize future scan speeds and comply with data retention best practices.

2. Bronze Layer (Raw)
Raw JSON files are ingested into a Delta Table using Unity Catalog. This layer serves as the "Source of Truth," preserving the original API response for auditability.

3. Silver Layer (Cleaned)
Data is refined using PySpark:
Flattening: Nested JSON structures (structs) are flattened into standard columns.
Typing: Timestamps are cast from ISO strings to TimestampType.
Idempotency: Implemented Delta MERGE (Upsert) logic to prevent data duplication during incremental runs.

4. Gold Layer (Business Logic)
The final analytical layer uses Spark SQL Window Functions (LAG()) to calculate:
Absolute Price Change ($)
Percentage Change (%)


Key Engineering Challenges Solved
1. Secure Distributed Authentication
Initially encountered UnsupportedOperationException and FileNotFound errors when trying to pass GCP credentials to Spark workers.
Solution: Moved away from legacy DBFS and implemented Unity Catalog Volumes for secure key storage. Utilized Direct JSON Injection of Service Account credentials into the Spark JVM to ensure seamless authentication across the distributed cluster.

2. Ensuring Data Idempotency
Standard .append() operations led to duplicate records when jobs were re-run.
Solution: Developed a MERGE INTO logic matching on event_timestamp. This ensures the pipeline is Idempotent—it can be executed multiple times without altering the final state unless new data is present.

4. Automated Workflow Orchestration
Moving from manual execution to a production-ready schedule.
Solution: Created a multi-task Databricks Workflow with a Directed Acyclic Graph (DAG). Task 2 (Transformation) only triggers if Task 1 (Ingestion) succeeds, creating a reliable circuit breaker.

Visualizations
The project includes a Databricks-native dashboard visualizing
Real-time Bitcoin Price Volatility.
Hourly percentage change trends.
