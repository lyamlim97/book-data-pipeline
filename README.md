# Book Data Pipeline Project

<details>
    <summary>Table of Contents</summary>
    <ol>
        <li>
            <a href="#introduction">Introduction</a>
            <ul>
                <li>
                    <a href="#built-with">Built With</a>
                </li>
            </ul>
        </li>
        <li>
            <a href="#project-architecture">Project Architecture</a>
        </li>
        <li>
            <a href="#getting-started">Getting Started</a>
            <ul>
                <li>
                    <a href="#prerequisites">Prerequisites<a>
                </li>
                <li>
                    <a href="#create-a-google-cloud-project">Create a Google Cloud Project<a>
                </li>
                <li>
                    <a href="#set-up-kaggle">Set up Kaggle<a>
                </li>
                <li>
                    <a href="#set-up-the-infrastructure-on-GCP-with-terraform">Set up the infrastructure on GCP with Terraform</a>
                </li>
                <li>
                    <a href="#set-up-Airflow">Set up Airflow</a>
                </li>
            </ul>
            <li>
                <a href="#data-ingestion">Data Ingestion</a>
            </li>
        </li>
    </ol>

</details>

## Introduction

In this project, I developed a data pipeline to process and load data from a Kaggle dataset containing bookstore information. This dataset is available at [Kaggle](https://www.kaggle.com/datasets/arashnic/book-recommendation-dataset/).

The primary objective of this project is to set up a data pipeline for extracting, storing, cleaning and visualizing the data automatically.

As the dataset is static, the pipeline is designed for a one-time process.

### Built With

- Dataset repo: [Kaggle](https://www.kaggle.com)
- Infrastructure as Code: [Terraform](https://www.terraform.io/)
- Workflow Orchestration: [Airflow](https://airflow.apache.org)
- Data Lake: [Google Cloud Storage](https://cloud.google.com/storage)
- Data Warehouse: [Google BigQuery](https://cloud.google.com/bigquery)
- Transformation: [DBT](https://www.getdbt.com/)
- Visualisation: TBD
- Programming Language: Python and SQL

## Project Architecture

Cloud infrastructure is set up with Terraform.
Airflow is run on a local docker container.

## Getting Started

### Prerequisites

1. A [Google Cloud Platform](https://cloud.google.com/) account.
2. A [kaggle](https://www.kaggle.com/) account.
3. Install [VSCode](https://code.visualstudio.com/) or any other IDE that works for you.
4. [Install Terraform](https://www.terraform.io/downloads)
5. [Install Docker Desktop](https://docs.docker.com/get-docker/)
6. [Install Google Cloud SDK](https://cloud.google.com/sdk)
7. Clone this repository onto your local machine.

### Create a Google Cloud Project

- Go to [Google Cloud](https://console.cloud.google.com/) and create a new project.
- Get the project ID and define the environment variable `GCP_PROJECT_ID` in the .env file located in the root directory
- Create a [Service account](https://cloud.google.com/iam/docs/service-account-overview) with the following roles:
  - `BigQuery Admin`
  - `Storage Admin`
  - `Storage Object Admin`
  - `Viewer`
- Download the Service Account credentials and store it in `terraform/google_credentials.json`
- You need to activate the following APIs [here](https://console.cloud.google.com/apis/library/browse)
  - Cloud Storage API
  - BigQuery API
- You will also need to enable billing for your project.

### Set up Kaggle

- A detailed description on how to authenicate is found [here](https://www.kaggle.com/docs/api)
- Define the API credentials KAGGLE_USER and KAGGLE_TOKEN in the .env file located in the root directory.

### Set up the infrastructure on GCP with Terraform

- Using VSCode, open the cloned project `book-data-pipeline`.
- In `variables.tf`, edit the default values of `variable "project"` and `variable "region"` to your preferred project ID and region.
- Open the terminal to the root project.
- In the terminal, natigate to the terraform folder using the command `cd terraform`.
- Initialise Terraform: `terraform init`
- Plan the infrastructure: `terraform plan`
- Apply the changes: `terraform apply`

### Set up Airflow

- Please confirm that the following environment variables are configured in `.env` in the root directory of the project.
  - `AIRFLOW_UID`. The default value is 50000
  - `KAGGLE_USERNAME`. This should be set from [Set up Kaggle](#set-up-kaggle) section.
  - `KAGGLE_TOKEN`. This should be set from [Set up Kaggle](#set-up-kaggle) section too
  - `GCP_PROJECT_ID`. This should be set from [Create a Google Cloud Project](#create-a-google-cloud-project) section
  - `GCP_BOOK_BUCKET=datalake-<GCP project id>`
  - `GCP_BOOK_WH_DATASET=book_analytics`
  - `GCP_BOOK_WH_EXT_DATASET=book_ext`
- Run `docker-compose up`.
- Access the Airflow dashboard by visiting `http://localhost:8080/` in your web browser. The interface will resemble the following. Use the username and password airflow to log in.

## Data Ingestion

Once you've completed all the steps outlined above, you should now be able to view the Airflow dashboard in your web browser.

In order to run a DAG, click on the play button.
