# Fuzzy Analytics (ELT) Pipeline 
![CI Status](https://github.com/congminh-de/fuzzy_analytics_pipeline/actions/workflows/ci.yml/badge.svg)

---

## Project Overview

As a practical exercise in modern data engineering, this project tackles the data challenges of Fuzzy Factory, a toy e-commerce business operating from 2012 to 2015. Seeking to bridge the gap between raw data and C-level strategy, I implemented an end-to-end ELT pipeline using Dagster, Polars, and dbt to orchestrate data movement from MySQL to BigQuery. This infrastructure supports a deep-dive analysis across five phases: Business Overview, Product, Customer, Channel, and Funnel. The final deliverables—structured SQL in BigQuery, Looker Studio dashboards, and Canva narrative reports—aim to demonstrate a professional, production-ready approach to data-driven storytelling. 
For more details, read these documents: [PRD](./docs/PRD.pdf) and [Project_Charter](./docs/Project_Charter.pdf).

## System Architecture & Data Flow

---

### Data Flow

Below is the end-to-end data flow of the **Fuzzy Analytics Pipeline**, illustrating the movement of data from source systems to the final analytics layer.

<p align="center">
  <img src="docs/Fuzzy_Analytics_Pipeline.png" width="900" alt="Fuzzy Analytics Pipeline Flow">
</p>

---

### Project Structure

```
fuzzy_pipeline/
├── dagster_home/              # Persistent storage for Dagster runs, logs, and sensor states
├── docs/                      # System architecture diagrams and documentation
├── fuzzy_dbt/                 # Transformation layer (dbt project)
│   ├── models/                # SQL definitions for tables (staging, intermediate, marts)
│   ├── dbt_project.yml        # Main dbt configuration file
│   ├── packages.yml           # External dbt packages declaration (e.g., dbt_utils)
│   └── profiles.yml           # Database connection profiles (BigQuery)
├── src/                       # Main Python source code
│   └── pipeline.py            # Orchestration logic (Assets, Jobs, and Sensors)
├── .env                       # Environment variables and secret credentials
├── .gitignore                 # Files and directories ignored by Git
├── bq_key.json                # Google Cloud Service Account key for BigQuery
├── dagster.yaml               # Dagster instance settings (Storage, Daemon, Scheduler)
├── docker-compose.yml         # Multi-container orchestration (App, Daemon, MySQL)
├── Dockerfile                 # Docker image build instructions
├── README.md                  # Project overview and setup instructions
├── requirements.txt           # Python library dependencies
├── seed_mysql.py              # Script to initialize mock data in MySQL source
└── workspace.yaml             # Dagster workspace definitions
```
---

## Dataset Overview

6 relational tables: orders, order_items, order_item_refunds, products, website_sessions, website_pageviews
* ~32K orders | ~40K order items | ~1.7K refunds | ~472K website sessions....
* Date range: March 2012 – March 2015
* 4 products launched progressively across the period

> **Note:** For a comprehensive technical deep-dive, including the full schema and data transformations, please refer to the [Data Dictionary](./docs/Data_Dictionary.pdf).

## Key Features

* **Automated End-to-End Orchestration:** Automates full data lifecycles from extraction to loading and transformation, ensuring seamless synchronization between MySQL, S3, and BigQuery.

* **Data Quality & Governance:** Implements automated dbt tests to validate business logic and integrity, preventing erroneous data from reaching downstream analytical reports.

* **Proactive Monitoring System:** Employs real-time sensors to track pipeline health, delivering instant email notifications upon task completion or critical system failures.

* **High-Performance Processing:** Leverages Polars for rapid data ingestion and dbt for cloud-native transformations, significantly reducing latency for e-commerce analytical workloads.

* **State Persistence & Portability:** Utilizes Docker and persistent storage to preserve run histories and configurations, ensuring reliability across different deployment environments.

* **Automated Data Lineage:** Generates dynamic lineage maps and documentation, providing clear visibility into complex data dependencies for improved system maintenance.

## Analysis Scope

| Phase | Focus |
|---|---|
| **Business Overview** | Revenue, profit, growth trajectory, refund health |
| **Product** | Product mix, AOV drivers, cross-sell, attach rate |
| **Customer** | Repeat behavior, purchase patterns, retention signals |
| **Channel** | Traffic sources, campaign efficiency, device performance |
| **Funnel** | Conversion funnel, landing page A/B tests, drop-off analysis |

## Dashboard & BI Report

| **Looker Studio Dashboard** | Interactive monitoring layer | [View Dashboard](https://datastudio.google.com/reporting/5d82c87a-7235-41a9-b536-884f7a63fd12) |

| **Canva BI Report** | Executive narrative report | [View Report](https://canva.link/fwlq6xbygip4n3s) |
