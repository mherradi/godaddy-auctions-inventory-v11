# 🌐  GoDaddy Domains Auctions Data Engineering Pipeline V1.1
&nbsp;&nbsp;&nbsp;This end-to-end Ingestion pipeline using Airflow & GCP , the Goal is to Set up an Airflow DAG on GCP that retrieves economic and social indicators from the World Bank’s public API, stores the raw data in Cloud Storage, and then loads it into BigQuery for analysis.
&nbsp;
&nbsp;

##  Tools and Platforms :

![GCP](https://img.shields.io/badge/GCP-4285F4?style=flat&logo=googlecloud&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-844FBA?style=flat&logo=terraform&logoColor=white)
![Google Cloud Run](https://img.shields.io/badge/Google%20Cloud%20Run-4285F4?style=flat&logo=googlecloud&logoColor=white)
![Google Cloud Composer](https://img.shields.io/badge/Google%20Cloud%20Composer-4285F4?style=flat&logo=googlecloud&logoColor=white)
![Compute Engine](https://img.shields.io/badge/Compute%20Engine-4285F4?style=flat&logo=googlecloud&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=flat&logo=googlebigquery&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![Bash](https://img.shields.io/badge/Bash-4EAA25?style=flat&logo=gnubash&logoColor=white)
![ELT Pipeline](https://img.shields.io/badge/ELT-Pipeline-2E8B57?style=flat)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)

&nbsp;
## What is a Domain, and why use GoDaddy as the Inventory Source?

&nbsp;&nbsp;&nbsp;A domain name is the human-readable address people use to reach a website or an online service, such as facebook.com. It is made up of labels separated by dots: in facebook.com, example is the registered name 
and .com is the top-level domain (TLD). 

<img width="841" height="365" alt="images" src="https://github.com/user-attachments/assets/c69e813b-9fd6-4335-9cbd-7f768e2fb2dd" />

This project focuses on domain names listed in GoDaddy's auction inventory. These listings provide a practical dataset for studying the domain aftermarket: domains offered through auctions or closeout listings
together with inventory attributes such as auction type, price, end time, and other available listing metadata.

## Project Aim and Objectives:

The aim of this project is to build a daily domain data warehouse: a reliable, continuously refreshed view of domains listed in GoDaddy's aftermarket inventory.

By collecting and preserving a daily snapshot of auction and closeout listings, the project creates a structured historical dataset that investors and data analysts 
can use to explore the domain market and identify potentially attractive investment opportunities.
&nbsp;
## Architecture Overview
This project  discovers source archives, downloads and extracts them, profiles and validates the records, standardizes the data into Parquet, loads daily snapshots into BigQuery, and builds analytics-ready models with dbt. 
Docker packages the workloads, Cloud Run Jobs executes them, Cloud Composer orchestrates them, and Terraform manages core infrastructure.
&nbsp;
&nbsp;


## Project Different Stages breakdown 
- Data Collection Module & Responsibilities
```Text
GoDaddy Auctions V.1.1/
|
|-- collect/
|   |-- discovery.py
|   |-- downloader.py
|   |-- extractor.py
|   |-- profiler.py
|   |-- quality_checks.py
|   |-- transform.py
|   `-- runner.py
|-- config/
|   `-- config.yaml
|-- data/
|   |-- raw/
|   |-- processed/
|   |-- transformed/
|   `-- reports/
```

| Component / Script | Description |
| :--- | :--- |
| **`discovery.py`** | Reads GoDaddy metadata, identifies target archives, records collection/source metadata, writes discovery information to GCS. |
| **`downloader.py`** | Downloads discovered ZIP files, validates size and ZIP integrity, uploads archives to GCS. |
| **`extractor.py`** | Extracts JSON from ZIP archives and writes processed objects to GCS. |
| **`profiler.py`** | Streams JSON records and profiles fields, types, nulls, numeric distributions, duplicates, and overlap. |
| **`quality_checks.py`** | Contains data quality validation logic used by the pipeline. |
| **`transform.py`** | Standardizes records, validates uniqueness and quality, writes Parquet, uploads transformed output. |
| **`runner.py`** | Orchestrates discovery, download, extraction, and transformation. |

- Terraform infrastructure :

Terraform files are in infrastructure/terraform/. They manage core resources including APIs, Artifact Registry, service accounts, IAM bindings, GCS, BigQuery, and Cloud Run Jobs.
```Text
GoDaddy Auctions V.1.1/
|
|-- infrastructure/
|   `-- terraform/
|       |-- main.tf
|       |-- providers.tf
|       |-- variables.tf
|       |-- outputs.tf
|       |-- iam.tf
|       |-- storage.tf
|       |-- bigquery.tf
|       |-- artifact_registry.tf
|       |-- cloud_run.tf
|       |-- composer.tf
|       |-- terraform.tfvars.example
|       `-- .gitignore
```
We deliberately consist a GCS bucket for terraform , where The backend bucket must exist before Terraform initializes the backend.
```Shell
gs://godaddy-auctions-v11-tfstate-49757842710
```
- dbt transformations layer :

After the initial loading of the data into the GCS bucket ,here we use dbt as transformation layer along with dbt BigQuery adapter, the goal is to define three models staging , intermediate , and marts models 
along with loading two dbt seeds which is keywords and suffixes .csv files.

```Text
GoDaddy Auctions V.1.1/
|
|-- dbt/
|   |-- analyses/
|   |-- macros/
|   |-- models/
|   |   |-- staging/
|   |   |-- intermediate/
|   |   `-- marts/
|   |-- seeds/
|   |-- snapshots/
|   |-- tests/
|   |-- dbt_project.yml
|   `-- packages.yml
```

