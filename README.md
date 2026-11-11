# 🌐  GoDaddy Domains Auctions Data Engineering Pipeline V1.1
&nbsp;&nbsp;&nbsp;This end-to-end Ingestion pipeline using Airflow & GCP, the Goal is to build a repeatable cloud data pipeline for GoDaddy Auctions inventory. It discovers source archives, downloads and extracts
them, profiles and validates the records, standardizes the data into Parquet, loads daily snapshots into BigQuery, and builds analytics-ready models with dbt. 
Docker packages the workloads, Cloud Run Jobs executes them, Cloud Composer orchestrates them, and Terraform manages core infrastructure.
&nbsp;
&nbsp;

##  ⚒️ Tools and Platforms :

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
## What is First a Domain, and why use GoDaddy as the Inventory Source?

&nbsp;&nbsp;&nbsp;A domain name is the human-readable address people use to reach a website or an online service, such as facebook.com. It is made up of labels separated by dots: in facebook.com, example is the registered name 
and .com is the top-level domain (TLD). 

<img width="841" height="365" alt="images" src="https://github.com/user-attachments/assets/c69e813b-9fd6-4335-9cbd-7f768e2fb2dd" />

This project focuses on domain names listed in GoDaddy's auction inventory. These listings provide a practical dataset for studying the domain aftermarket: domains offered through auctions or closeout listings
together with inventory attributes such as auction type, price, end time, and other available listing metadata.

## 🎯 Project Aim and Objectives:

The main goal is to build a daily domain inventory warehouse that provides a consistent and historical view of domains listed in GoDaddy's aftermarket inventory.
Instead of looking at a single list that changes over time, the pipeline collects a dated snapshot and keeps it as part of the project's history.
&nbsp;
## 🏗️ Pipeline Architecture Overview

<img width="1762" height="916" alt="Pipeline Architecture" src="https://github.com/user-attachments/assets/120de951-3187-4dd9-ad7e-d999b303e728" />

&nbsp;

This project  discovers source archives, downloads and extracts them, profiles and validates the records, standardizes the data into Parquet, loads daily snapshots into BigQuery, and builds analytics-ready models with dbt. 
Docker packages the workloads, Cloud Run Jobs executes them, Cloud Composer orchestrates them, and Terraform manages core infrastructure.
&nbsp;
&nbsp;


## 📂 Project Different Stages breakdown 
- **Data Collection Modules & Responsibilities**
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

- **Terraform infrastructure :**

The project's core Google Cloud infrastructure is described in infrastructure/terraform/. Terraform makes infrastructure changes reviewable and repeatable instead of relying entirely on manual configuration in the cloud console.

The Terraform configuration manages resources such as required APIs, service accounts, IAM bindings, GCS, BigQuery, Artifact Registry, and Cloud Run Jobs.
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
- Terraform workflow:

On the Windows development machine, Terraform was installed through WinGet and invoked by its full path because it was not available on PATH:
```Shell
$Terraform = "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\Hashicorp.Terraform_Microsoft.Winget.Source_8wekyb3d8bbwe\terraform.exe"
```
Move into the Terraform directory:
```Shell
Set-Location .\infrastructure\terraform
```
Initialize the backend and validate the configuration:
```Shell
& $Terraform init
& $Terraform fmt
& $Terraform validate
& $Terraform plan
```
Review the plan carefully before applying changes:
```Shell
& $Terraform apply
```

- **dbt transformations layer :**

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
- **Docker, Artifact Registry, and Cloud Run :**

Two images are built independently, the first is Dockerfile for Ingestion image using the Data Collection Modules, the second is dbt.Dockerfile that copies the dbt project and production profile, and starts the dbt CLI.

```Text
GoDaddy Auctions V.1.1/
|
|-- docker/
|   |-- Dockerfile
|   |-- dbt.Dockerfile
|   |-- .dockerignore
|   `-- profiles/
|       `-- profiles.yml
```


## 🚀 Deployment and Execution Guide 

The ingestion pipeline and dbt transformations project run as separate workloads. Docker packages each workload with its own dependencies so that it can execute consistently in the cloud.

- Ingestion Image:
  
The ingestion image is built from docker/Dockerfile. It uses python:3.13-slim, installs the dependencies listed in requirements-runtime.txt, copies the collect/ and config/ directories, and starts the collection workflow with
```Shell
python -m collect.runner
```
Build and smoke-test the image:
```Shell
docker build -f .\docker\Dockerfile -t godaddy-auctions-processor:v1.1.0 .
docker run --rm godaddy-auctions-processor:v1.1.0 --help
```
- dbt Image:

The dbt image is built from docker/dbt.Dockerfile. It uses python:3.13-slim, installs Git and the required dbt packages, copies the dbt project and production profile, and starts the dbt CLI.

```Shell
docker build -f .\docker\dbt.Dockerfile -t godaddy-auctions-dbt:v1.1.3 .

docker tag godaddy-auctions-dbt:v1.1.3 `
  europe-west9-docker.pkg.dev/godaddy-auctions-inventory-v11/godaddy-auctions-v11/auction-dbt:v1.1.3

docker push `
  europe-west9-docker.pkg.dev/godaddy-auctions-inventory-v11/godaddy-auctions-v11/auction-dbt:v1.1.3
```

Afterward Cloud Run Jobs execute the containerized workloads without requiring a continuously running server.


| Job | Responsibility |
|---|---|
| `godaddy-auctions-processor` | Runs discovery, download, extraction, and transformation |
| `godaddy-auctions-dbt` | Runs `dbt build` |


## 🎼 Orchestrate the pipeline with Cloud Composer

The individual stages can run independently, but a production-style pipeline needs a way to execute them in the correct order. Cloud Composer provides the managed Airflow environment used to coordinate the workflow.
The DAG is defined in dags/godaddy_auctions_dag.py and has the ID:

```Shell
godaddy_auctions_pipeline
```

- What happens in each task?

| Task | Job |
|---|---|
| `run_ingestion` | Airflow uses CloudRunExecuteJobOperator to start the ingestion Cloud Run Job. The task passes the DAG logical date as the snapshot date, so the collection is associated with the intended daily partition. |
| `load_bigquery` | After ingestion succeeds, BigQueryInsertJobOperator loads the transformed Parquet object into the matching BigQuery partition. |
| `run_dbt` | Once the warehouse load succeeds, Airflow starts the dbt Cloud Run Job. This task builds the dbt models and executes the project's tests. |


## 📋 Proof of Concept & Screenshots  :
In the following we will include screenshots that will illustrate Airflow Composer Up & Running in a Healthy status. 

&nbsp;
<img width="1911" height="331" alt="image" src="https://github.com/user-attachments/assets/e6ab19a6-257c-4560-b0f1-ef8629f1db81" />

&nbsp;
Airflow Ingestion Dag Graph showcase the different ingestion layersr from extraction, bigquery loading , and data quality checks with one run succeeded.

&nbsp;
<img width="1918" height="647" alt="image" src="https://github.com/user-attachments/assets/2f81ed8a-2f3e-4317-a045-be3fd5e5d3df" />

&nbsp;
<img width="1916" height="827" alt="image" src="https://github.com/user-attachments/assets/d567d453-af00-4068-ac88-8727d0d02566" />

&nbsp;
As result we can see now our data successfully ingested into BigQuery Warehouse and we can run the following SQL script:
&nbsp;
```SQL
SELECT * FROM `godaddy-auctions-inventory-v11.godaddy_auctions.auction_inventory` WHERE snapshot_date = "2026-11-10" LIMIT 1000 ;
```

&nbsp;
<img width="1915" height="797" alt="image" src="https://github.com/user-attachments/assets/34b54777-8d44-45c0-87b1-40592263b152" />


We can see as well the result of the staging layer which prepares the source data for downstream use and provides a consistent starting point for transformations.
&nbsp;
```SQL
SELECT * FROM `godaddy-auctions-inventory-v11.godaddy_auctions_dbt.stg_auction_inventory` LIMIT 1000 ;
```

&nbsp;
<img width="1918" height="836" alt="image" src="https://github.com/user-attachments/assets/e93eac07-8871-439c-aa86-d31eeb76b239" />

The intermediate layers applies reusable transformation logic between the source-oriented staging layer and the final analytical models.
&nbsp;
```SQL
SELECT * FROM `godaddy-auctions-inventory-v11.godaddy_auctions_dbt.int_auction_inventory` LIMIT 1000;
```

&nbsp;
<img width="1913" height="757" alt="image" src="https://github.com/user-attachments/assets/ef7a51c4-1851-419d-9be4-7c7e5ff389b4" />

&nbsp;
The marts layer provide tables designed around specific analytical use cases, in the following we can see table designed only for 4 letters domains.
```SQL
SELECT * FROM `godaddy-auctions-inventory-v11.godaddy_auctions_dbt.mart_four_letters_domains` where pure_letter=true LIMIT 1000 ;
```

&nbsp;
<img width="1913" height="777" alt="image" src="https://github.com/user-attachments/assets/fa4561e3-7bb2-4c32-b647-7b174dd2e823" />

## 💰 Cost management & Hibernation :
This project uses several managed Google Cloud services, and each can contribute to the overall cost. Composer was created in development mode with hibernation selected for periods when it is not in use.

Hibernation can reduce many Composer costs, but it does not eliminate every charge. DAG scheduling stops while Composer is hibernated, while services such as Cloud Run, BigQuery, GCS, Artifact Registry, networking, and logging may continue to incur costs.

## 🧑‍💻 About Me
 Hey , My name is Mohammed Herradi Welcome to my Github I hope you find the GoDaddy Inventory Warehouse Project Interesting. I’m a Data Engineering Enthusiast with a strong interest in building reliable, scalable, and production-ready data solutions. My anthem in life is "Always Learning" . Today, my main focus is Data Engineering, particularly the design and development of end to end data pipelines delivering data products. Always learning. Always building. Always looking for better ways to work with data .


Interested in this project, domain-market data, data engineering, or potential collaboration?

Let's connect on LinkedIn:

[![Connect on LinkedIn](https://img.shields.io/badge/LinkedIn-Connect%20with%20me-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/mohammed-herradi/)

