Ingestion Layer
Overview
The ingestion layer is responsible for collecting raw data for the RecoMart recommendation pipeline from multiple source types and storing it in a structured local data lake. It supports both API-based and file-based ingestion, with logging, retry handling, manifest generation, and bronze-layer standardization for downstream processing.
This implementation is designed to improve:

reproducibility
auditability
fault tolerance
downstream pipeline readiness

Data Sources
Source 1: API product/catalog data
The pipeline ingests product and category data from the DummyJSON REST API.
Datasets ingested:

product catalog
product categories

Key behaviors:

fetches paginated API data
retries transient failures with backoff
supports offline fallback payloads if the API is unavailable
stores raw API responses as JSON
writes bronze-layer parquet outputs for downstream use

Source 2: CSV interaction/catalog data
The pipeline ingests interaction and catalog data from the Retailrocket dataset.
Datasets ingested:

events.csv
category_tree.csv
item_properties_part1.csv
item_properties_part2.csv

Key behaviors:

downloads source files
validates expected files exist
copies raw CSVs into the local data lake
combines item property files for bronze processing
writes standardized parquet outputs for downstream use

Raw Storage Layout
Raw data is stored using a source-partitioned and time-partitioned folder structure:
data/raw/<source_name>/load_date=YYYY-MM-DD/load_hour=HH/
Example paths

data/raw/dummyjson/load_date=2026-04-28/load_hour=15/
data/raw/retailrocket/load_date=2026-04-28/load_hour=15/

This layout enables:

clear source separation
ingestion batch traceability
timestamp-based lineage
reproducible downstream processing

Bronze Layer Outputs
After ingestion, raw data is standardized and written to the bronze layer as parquet files.
Example bronze outputs

data/bronze/dummyjson/products.parquet
data/bronze/dummyjson/categories.parquet
data/bronze/retailrocket/events.parquet
data/bronze/retailrocket/category_tree.parquet
data/bronze/retailrocket/item_properties.parquet

Logging and Auditability
Each ingestion pipeline writes structured logs and metadata artifacts to support monitoring and audit trails.
Logs

stored as JSONL files under logs/
capture pipeline stage, status, batch ID, timestamp, and error details

Manifests

stored under metadata/
record batch-level details such as source, ingestion timestamp, raw output location, and files processed

Reliability Features
The ingestion layer includes several controls to improve robustness:

retry logic for transient failures
structured success/failure logging
expected-file validation for file-based ingestion
fallback support for API unavailability
batch metadata added to downstream datasets

Summary
The ingestion layer supports two source types:

API product/catalog data
CSV interaction/catalog data

It stores raw data in a structured source-and-timestamp-partitioned layout and produces bronze-layer datasets that are ready for validation, feature engineering, and model training.

=====================================================================
Validation Layer
This module implements the data profiling and validation stage of the RecoMart end-to-end recommendation pipeline. Its purpose is to ensure that ingested datasets are complete, well-structured, and suitable for downstream preparation, transformation, feature engineering, and model training, which is explicitly required in the assignment brief .
Objective
The assignment requires the validation layer to perform automated checks for:

missing values
duplicate records
schema mismatches
range and format validation
generation of a Data Quality Report (PDF)

This implementation aligns with those requirements by using automated validation logic in Python/pandas and by generating structured issue logs, summaries, JSON reports, and a PDF report .
What This Validation Layer Checks
The validation workflow is designed to validate both raw and bronze datasets discovered from the project data folders. The script identifies files under data/raw and data/bronze, then executes validation checks across the discovered sources .
Key validations include:

file discovery and presence checks
schema validation
missing value detection
duplicate detection
allowed domain checks such as valid event types
format validation such as timestamps
numeric range validation for fields like ratings, prices, and stock where applicable

The script also supports remediation and revalidation, meaning failed datasets can be auto-corrected where possible and then checked again to confirm whether quality has improved .
Datasets Covered
Based on the validation run, the module checks multiple project datasets, including:

RetailRocket event data
category tree data
item properties data
DummyJSON product data
bronze parquet outputs for products and categories

This supports the assignment expectation of handling multiple data sources in a maintainable pipeline for RecoMart’s recommendation system .
Outputs Generated
Each validation run produces:

validation issue files in CSV format
dataset summary files in CSV format
a fix log for remediation actions
a JSON data quality report
a PDF data quality report
structured validation logs

The PDF report includes:

initial validation results
revalidation results
before-vs-after comparison
final conclusion on overall validation status

Example Run Outcome
A sample execution shows:

initial validation status: FAIL
final validation status after remediation: PASS

This demonstrates that the validation layer not only detects data quality issues, but also supports corrective action and verification.
Project Relevance
This validation module contributes directly to the assignment’s Data Pipeline Implementation component, which covers ingestion, storage, validation, and transformation and carries 40% of the total marks . It also supports the documentation and reproducibility expectations by maintaining logs, reports, and a modular project structure .
Suggested Repository Location
Plain textsrc/validation/
    run_validation.py

reports/validation/
    validation_issues_initial_<run_id>.csv
    validation_summary_initial_<run_id>.csv
    validation_issues_revalidated_<run_id>.csv
    validation_summary_revalidated_<run_id>.csv
    fix_log_<run_id>.csv
    data_quality_report_<run_id>.json
    data_quality_report_<run_id>.pdf

logs/
    validation_log_<run_id>.jsonl

How to Use
Run the validation script after ingestion and raw storage are completed, and before data preparation begins. This ensures that only validated datasets move into the downstream stages of the pipeline, which is consistent with the required orchestration flow in the assignment: ingestion → validation → preparation → transformation → feature store → model training .


=====================================================================
Data Preparation Layer
This module implements the data preparation and exploratory data analysis (EDA) stage of the RecoMart recommendation pipeline. Its purpose is to transform validated raw datasets into clean, analysis-ready datasets that can be used in downstream transformation, feature engineering, and model training. This directly aligns with the assignment requirement to perform data cleaning, preprocessing, categorical handling, normalization of numerical variables, and EDA on user-item interaction data
Objective
The preparation layer is designed to:

clean and standardize raw interaction and product data
prepare user-item interaction datasets for recommendation use cases
analyze interaction behavior and product characteristics
generate prepared datasets ready for transformation
produce summary outputs and plots for documentation and reporting

This supports the assignment deliverables of a script/notebook demonstrating cleaning and EDA, summary plots, and a prepared dataset ready for transformation
Data Preparation Activities
Based on the preparation script, this layer performs the following tasks:
1. Interaction Data Cleaning
The interaction dataset is cleaned and standardized before being saved as interactions_prepared.csv. The workflow includes column handling, filtering valid events, and preparing the dataset for analysis and recommendation modeling. The prepared interaction dataset is successfully generated and saved as part of the run output
2. Product and Category Preparation
The script also prepares product and category datasets and saves them as:

products_prepared.csv
categories_prepared.csv
retailrocket_item_snapshot.csv

These prepared outputs are explicitly shown in the execution results, confirming that the preparation layer produces structured datasets for downstream use
3. Category Standardization
Category values are cleaned by converting them to string format, trimming whitespace, lowercasing text, removing blanks, and dropping duplicates. This improves consistency and helps make categorical data easier to use in downstream transformations
4. Prepared Data Validation
After preparation, the script reruns validation on the prepared outputs for:

interactions_prepared
products_prepared
categories_prepared

It generates prepared validation issue and summary reports, helping ensure that the outputs are suitable for the next pipeline stage
Exploratory Data Analysis (EDA)
The assignment asks for EDA showing interaction distributions, item popularity, and sparsity patterns . This layer addresses that through multiple visual analyses:
Interaction Analysis
The script creates plots for:

interaction type distribution
daily interaction volume
top 20 most popular items
user activity distribution

These outputs directly support the recommendation-system context by showing how users interact with items and which items are most frequently engaged with
Product Analysis
The script also generates product-focused plots for:

product price distribution
product rating distribution
top product categories

These plots help profile the catalog data and understand item-side characteristics before feature engineering
Sparsity Analysis
To understand recommendation data density, the script creates a sampled user-item matrix and visualizes it as a sparsity heatmap. The run output reports a sample matrix shape of (50, 44) and a sample sparsity of 0.7236, which is useful for understanding how sparse the interaction data is for collaborative filtering scenarios
Outputs Generated
A sample run of the preparation layer produces the following key outputs:

data/prepared/interactions_prepared.csv
data/prepared/retailrocket_item_snapshot.csv
data/prepared/products_prepared.csv
data/prepared/categories_prepared.csv
data/quality/prepared_validation_issues_<timestamp>.csv
data/quality/prepared_validation_summary_<timestamp>.csv
reports/eda/eda_summary_<timestamp>.json

These outputs are explicitly shown in the script execution results
Example Summary Statistics
The EDA summary includes large-scale interaction data, including:

2,755,641 interaction rows
1,407,580 unique users
235,061 unique items
a tracked sample sparsity metric
quarantined bad transaction rows for data quality monitoring

These metrics show that the preparation layer is not only cleaning data but also producing dataset-level summaries useful for analysis and reporting
Role in the End-to-End Pipeline
This preparation layer sits after validation and before transformation. In the assignment pipeline, it supports the broader flow of:
ingestion → validation → preparation → transformation → feature store → model training
Its main contribution is to convert raw multi-source recommendation data into cleaned, validated, and EDA-documented assets that are ready for downstream feature creation.

==========================================================================================================
