# 📌 1. Introduction
This project showcases a **near-real-time ELT (Extract, Load, Transform) pipeline** built entirely with open-source tools.

The source data comes from the [NYC Taxi & Limousine Commission (TLC) Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), provided as Parquet files.

Key components include:  
- `Ingestion:` A Python producer reads the Parquet data line-by-line and streams it to a **Kafka** topic to simulate real-time data flow.  
- `Loading:` **Spark Structured Streaming** consumes data from Kafka, applies lightweight processing, and loads it into a **DWH**.  
- `Transformation:` **dbt** is used to transform and model the raw data across multiple layers: raw → staging → intermediate → mart.
- `Orchestration:` **Airflow** manages and schedules the entire transformation workflow to ensure reliability and automation.  

---

# 🏗 2. Architecture
## 2.1 Original Pipeline
![Original Pipeline](readme/pipeline-1.png)

## 2.2 Alternative Pipeline  
Since the only available data source is a Parquet file (with no direct database connection), Debezium cannot be used. Instead, a Python producer reads the file line-by-line and sends each record to a Kafka topic to simulate real-time data ingestion.
![Alternative Pipeline](readme/pipeline-2.png)

---

# 🚧 3. Data Modeling
Below is the dbt model lineage illustrating the flow of transformations:
![dbt Lineage](readme/dbt-lineage.png)

And here is the star schema design used in the data warehouse to support analytical queries:
![Star Schema](readme/star-schema.png)

---

# 📂 4. Project Structure
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
│       │   ├── ...   
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

# 🚀 5. Setup
## 5.1 Prerequisites  
Before starting, please ensure you have:  
- Docker Desktop installed and running on your machine.  

## 5.2 Start Services  
Start the required services in the following order by running these commands in your terminal:

```bash
docker network create common-net
docker-compose -f docker-compose-kafka.yml up -d
docker-compose -f docker-compose-spark.yml up -d
docker-compose -f docker-compose-dwh-dbt-airflow.yml up -d
```
## 5.3 Accessing Services
- `Kafka UI`: http://localhost:8080
- `Spark Master UI`: http://localhost:8082
- `Spark Worker 1 UI`: http://localhost:8083
- `Spark Worker 2 UI`: http://localhost:8084
- `Airflow Webserver`: http://localhost:8085
- `Postgres DWH` is accessible on port 5432 with credentials:
  - User: postgres
  - Password: 123456
  - Database: taxi_dwh

## 5.4. Running the Pipeline
