# End-to-End-Azure-Data-Engineering-Project
This project demonstrates a complete data engineering pipeline using Azure technologies — Azure Data Factory (ADF), Azure Databricks, and Azure Data Lake Storage (ADLS). It simulates a real-world scenario of ingesting, transforming, and analyzing trip data.
Created three containers(bronze, silver and gold) in AZURE DATA LAKE GEN 2

Ingested data from HTTP sources into ADLS.

Built ADF Dynamic pipelines for scheduled data movement and orchestration.
Actvities(GetMetadata, Foreach, If condition)
Developed Databricks notebooks for data cleansing, aggregation, and transformations using PySpark.
Managed vs External Tables
Delta Lake , Delta Live Tables
Stored transformed data into curated zones in ADLS.
