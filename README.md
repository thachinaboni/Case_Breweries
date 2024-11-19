# Breweries Case

This repository contains a data pipeline designed to fetch, transform, and persist data from the [Open Brewery DB API](https://www.openbrewerydb.org/) into a Data Lake following the Medallion Architecture. The solution processes raw brewery data and stores it in three distinct layers: Bronze, Silver, and Gold. Below is an overview of the implemented solution:

## Objective
The goal of this project is to showcase the ability to consume data from an API, perform transformations, and persist the results in a data lake with a clear architecture. The pipeline follows the Medallion Architecture with the following layers:

- Bronze Layer: Store raw data fetched from the API.
- Silver Layer: Transform data to a columnar format (Parquet), partitioned by brewery location.
- Gold Layer: Aggregate the data to provide an analytical view, such as the quantity of breweries per type and location.

## Pipeline Overview
Data Fetching: Data is retrieved from the [Open Brewery DB API](https://www.openbrewerydb.org/), which provides information on breweries, including their names, types, locations, and coordinates.

## Data Transformation:
Raw data is transformed into a structured format using Pandas.
Data is cleaned by handling missing values and rounding numeric columns like latitude and longitude.
The Silver Layer stores the data in Parquet format, partitioned by state to optimize query performance.
The Gold Layer provides aggregated insights into brewery types and their locations.
Technologies Used
Language: Python with Pandas for data processing.
Orchestration: The pipeline can be orchestrated with Airflow for scheduling, retries, and error handling.
Data Lake Storage: Data is stored in a local file system or can be configured for cloud storage services like AWS S3 or Azure Blob Storage.
Containerization: The pipeline is containerized using Docker to ensure portability and ease of deployment.
Steps to Run the Pipeline
Clone the repository:
```bash
git clone <repository-url>
```

```bash
pip install -r requirements.txt
```
Run the Pipeline: The pipeline can be executed with:
```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker compose down --volumes --remove-orphans
docker compose up -d
```
After this, you can have the localhost. To access Airflow, login and the password are the same:
```
airflow
```

This will fetch data from the API, process it, and store it in the specified layers on your local machine or cloud storage.

## Monitoring and Alerting
For monitoring and alerting, the pipeline can be integrated with tools such as Airflow for scheduling, retries, and error handling. In case of pipeline failures, notifications can be sent via email or integrated with monitoring platforms like Prometheus or Datadog.

## Containerization
Full containerization with Docker.

## Design Considerations
Partitioning: The data is partitioned by state in the Silver Layer to optimize read and write operations.
Error Handling: The pipeline includes error handling for API requests and data processing steps to ensure reliability.

## Future Enhancements
- Scalability: The pipeline can be extended to handle larger datasets by scaling the Spark jobs.
Additional Aggregations: More complex aggregations or transformations can be added to the Gold Layer for deeper analytics.
- Cloud: AWS S3 or Azure Blob Storage for storing Parquet files. Airflow can be set up on cloud instances or using managed services like AWS Managed Workflows for Apache Airflow.
- PySpark: To be able to obtain the result in Pyspark, it is necessary to make some changes to Docker and include the Dockerfile. The code with Pyspark is available in the folder: notebooks

This repository demonstrates a complete data pipeline architecture, from raw data collection to analytical insights, using modern tools and best practices for data engineering.