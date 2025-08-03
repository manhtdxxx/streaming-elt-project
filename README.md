# 📌 1. Introduction
- This project showcases a **near-real-time ELT (Extract, Load, Transform) pipeline** built entirely with open-source tools.

- The source data comes from the [NYC Taxi & Limousine Commission (TLC) Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), provided as Parquet files.

- Key components include:  
  - `Ingestion:` A Python producer reads the Parquet data line-by-line and streams it to a **Kafka** topic to simulate real-time data flow.  
  - `Loading:` **Spark Structured Streaming** consumes data from Kafka, applies lightweight processing, and loads it into a **DWH**.  
  - `Transformation:` **dbt** is used to transform and model the raw data across multiple layers: `raw → staging → intermediate → mart`.
  - `Orchestration:` **Airflow** manages and schedules the entire transformation workflow to ensure reliability and automation.  

---

# 🏗 2. Architecture
## 2.1 Original Pipeline
![Original Pipeline](readme/pipeline-1.png)

## 2.2 Alternative Pipeline  
Since the only available data source is a Parquet file (with no direct database connection), Debezium cannot be used. Instead, a Python producer reads the file line-by-line and sends each record to a Kafka topic to simulate real-time data ingestion.
![Alternative Pipeline](readme/pipeline-2.png)

---

# 📂 3. Project Structure
```text
streaming-elt-project/
│
├── data/                                # Sample dataset files (Parquet) used for streaming simulation
├── docker/                              # Custom Dockerfiles & extra configs for docker-compose services
├── experiments/                         # Notebooks & scripts for testing pipeline
├── readme/                              # Documentation assets (diagrams, images, and additional notes)
├── src/
│   ├── streaming_scripts/               # Python scripts for Kafka producer and Spark Structured Streaming consumer
│   │   └── ...                         
│   └── dbt/                            
│       ├── nyc_taxi/                    # dbt project
│       │   ├── macros                     # dbt reusable SQL snippets/functions
│       │   ├── models                     # dbt data transformation models, organized into layers:
│       │   │   └── staging                  - clean and standardize raw source data
│       │   │   └── intermediate             - join, enrich, and aggregate staging data
│       │   │   └── mart                     - business-ready tables for analytics & BI
│       │   ├── dbt_project.yml            # dbt project configuration
│       │   └── packages.yml               # dbt dependencies
│       ├── profiles.yml                 # Database connection settings for dbt
│       └── dag.py                       # Airflow DAG to run dbt commands (dbt run, dbt test)
│
├── docker-compose-dwh-dbt-airflow.yml   # Compose file for DWH, dbt, and Airflow services
├── docker-compose-kafka.yml             # Compose file for Kafka cluster, Schema Registry, and Kafka UI
├── docker-compose-spark.yml             # Compose file for Spark cluster
```

---

# 🚧 4. Data Modeling
Below is the dbt model lineage illustrating the flow of transformations:
![dbt Lineage](readme/dbt-lineage.png)

And here is the star schema design used in the data warehouse to support analytical queries:
![Star Schema](readme/star-schema.png)

---

# 🚀 5. Setup
