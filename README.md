🧭 Overview

This project implements a production-ready data ingestion and transformation pipeline using Apache Airflow (Celery Executor) to orchestrate workflows across Amazon S3 and AWS Glue.

The pipeline is built around a clear separation of concerns:

Amazon S3 serves as the central data lake storage layer

AWS Glue performs scalable, distributed ETL transformations

Apache Airflow (Celery Executor) manages orchestration, distributed task execution, dependencies, retries, and monitoring

The solution follows industry best practices for cloud-native ETL pipelines and is designed to scale horizontally as workload increases.

🔑 Key Concepts
🛠 Apache Airflow (Celery Executor)

Acts as the orchestration layer

Uses Celery Executor for distributed task execution

Enables horizontal scaling via multiple workers

Manages task dependencies and execution order

Handles retries, failures, and operational logging

Runs locally using Docker Compose for development and testing

☁ Amazon S3

Serves as the persistent storage layer

Stores both raw input data and processed outputs

Acts as the primary integration point between pipeline components

🔄 AWS Glue

Executes distributed ETL jobs using Apache Spark

Performs data transformation and enrichment

Writes curated datasets back to Amazon S3

Operates independently from Airflow’s runtime environment

⚙️ Pipeline Flow
1️⃣ S3 File Detection

Airflow waits for data to arrive in a predefined S3 location

Uses non-blocking sensor execution (reschedule mode) to optimise worker resource usage

2️⃣ Technical Validation

Validates file availability and integrity

Enables early failure detection to avoid unnecessary downstream processing

3️⃣ Schema Validation

Ensures incoming data conforms to the expected structure

Prevents malformed or incompatible data from entering the pipeline

4️⃣ Glue Job Execution

Airflow triggers an AWS Glue ETL job

Glue performs transformations and enrichment at scale

Processed data is written to a curated S3 location

🏗️ Execution Model

The solution leverages the Celery Executor architecture, which includes:

Airflow Webserver

Airflow Scheduler

Celery Workers

Message Broker (Redis or RabbitMQ)

Metadata Database

This architecture enables:

Parallel task execution

Horizontal scaling of workers

Improved reliability under increased workloads

Clear separation of orchestration and execution layers

🔐 Security & IAM Model

The pipeline follows a least-privilege IAM design, ensuring secure and auditable access.

Airflow IAM Identity

Starts AWS Glue jobs

Monitors Glue job execution status

Interacts with Amazon S3 for orchestration purposes

Glue Execution Role

Reads source data from Amazon S3

Writes processed data back to Amazon S3

Publishes execution logs to Amazon CloudWatch

Interacts with the Glue Data Catalog when required

Airflow and Glue operate using separate IAM roles, ensuring clear security boundaries and minimal permission overlap.

🐳 Local Development

The project runs locally using Docker Compose with Celery-based distributed execution.

Components

Airflow Webserver

Airflow Scheduler

Celery Workers

Redis (Message Broker)

Airflow Metadata Database

Custom Airflow image with required provider packages

🧪 Error Handling & Reliability

Idempotent task design to support safe re-execution

Explicit retry logic and retry delays

Distributed execution via Celery workers

Glue job failures are propagated back to Airflow

Full operational visibility via Airflow UI and CloudWatch logs

📈 Scalability & Extensibility

The pipeline is designed to scale and evolve:

Horizontal scaling via additional Celery workers

Additional Glue jobs can be added without DAG redesign

Multiple S3 data sources can be integrated

Downstream consumers can be introduced without changing ingestion logic

Suitable for both batch and near-real-time ingestion patterns
