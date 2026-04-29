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
Feature Engineering and Feature Store
This module builds recommendation-ready features from the prepared RecoMart datasets and manages them through a lightweight custom feature store.
It sits between data preparation and model training in the pipeline. Its purpose is to transform cleaned interaction and product data into reusable, versioned feature tables that support both offline training and inference-time retrieval. This directly aligns with the assignment requirements for feature engineering, feature storage, metadata documentation, and versioned feature access .

Overview
RecoMart’s recommendation pipeline requires features that can support both:

Collaborative filtering models such as SVD and item-based collaborative filtering
Content-based recommenders using product attributes and text-derived signals

This layer converts prepared datasets into stable, joinable features for those use cases. The prepared inputs currently include:

interactions_prepared.csv
products_prepared.csv
categories_prepared.csv
retailrocket_item_snapshot.csv

The preparation stage validates these outputs and reports them as passing, with approximately:

2,755,641 interaction rows
194 product rows
24 category rows


Objectives
This module fulfills two assignment sections:
1. Feature Engineering and Transformation
Create features suitable for recommendation algorithms, including:

user activity frequency
user/item aggregate engagement signals
co-occurrence and similarity-friendly features
content attributes for item representation

The assignment explicitly calls for transformation scripts, structured transformed outputs, and a summary of feature logic .
2. Feature Store
Implement a simple feature store using a custom metadata registry, including:

documented feature names
source datasets and columns
transformation logic
versioned retrieval for training and inference

These are direct feature store deliverables in the assignment .

Inputs
This layer consumes prepared data produced by the data preparation stage.
Primary input files

data/prepared/interactions_prepared.csv
data/prepared/products_prepared.csv
data/prepared/categories_prepared.csv
data/prepared/retailrocket_item_snapshot.csv

Expected prepared schemas
The preparation notebook validates key interaction fields such as:

user_id
item_id
event
event_weight
event_ts

The product preparation flow also expects product metadata such as:

id
title
category
price
optional brand


What This Module Produces
This layer generates structured feature outputs for downstream recommendation models.
Typical outputs

data/features/<version>/user_features.csv
data/features/<version>/item_features.csv
data/features/<version>/interaction_features.csv
data/features/<version>/content_features.csv
data/features/<version>/feature_registry.json
data/features/<version>/feature_build_summary.json

These outputs provide a reusable feature layer for both experimentation and reproducible pipeline runs.

Recommended Repository Structure
A clean layout for this layer is:

src/features/

build_features.py
feature_logic.py
feature_registry.py
README.md


data/prepared/

prepared inputs from EDA/prep


data/features/

versioned feature outputs


reports/

feature summaries and diagnostics



This structure also supports the assignment expectation of organized pipeline stages and clear documentation .

Feature Groups
User Features
User-level features summarize engagement behavior and preference intensity.
Examples:


user_interaction_count
Total number of interactions for a user


user_unique_items
Number of distinct items the user interacted with


user_avg_event_weight
Average weighted engagement score across the user’s interactions


user_transaction_count
Number of purchase events for the user


user_view_count
Number of view events


user_addtocart_count
Number of add-to-cart events


user_activity_frequency
Interaction rate over time


user_last_activity_ts
Most recent interaction timestamp


Item Features
Item-level features summarize popularity and engagement quality.
Examples:


item_interaction_count
Total interactions received by an item


item_unique_users
Number of unique users who interacted with the item


item_avg_event_weight
Mean weighted interaction score for the item


item_transaction_count
Number of transaction events


item_addtocart_count
Number of add-to-cart events


item_view_count
Number of view events


item_popularity_rank
Rank based on weighted engagement volume


Interaction Features
Interaction-level features preserve user-item behavior needed by collaborative models.
Examples:

user_id
item_id
event
event_weight
event_ts
event_date
repeat_interaction_flag
interaction_recency_days

Content Features
Content features support item similarity and content-based recommendation.
Examples:

product_id
title
category
brand
price
price_bucket
content_text_combined

The product preparation logic shows title, category, price, and optional brand as key fields for item representation .

Feature Engineering Logic
The feature logic follows a few practical rules:


Aggregate interaction logs into stable user and item summaries
Behavioral events are grouped by user and item to produce recommendation-ready signals.


Preserve entity keys for safe joins
user_id and item_id remain the primary join keys across feature tables.


Use weighted behavioral signals
The prepared interaction dataset includes event_weight, enabling different event types to contribute differently to model inputs .


Create content-based item representations
Product text and categorical fields are combined into a content profile for similarity-based recommenders.


Version every feature build
Each feature run is stored in a separate versioned folder for reproducibility and lineage.



Feature Store Design
This project uses a custom lightweight feature store rather than Feast.
Why a custom feature store?
A custom registry is enough for this assignment because it:

keeps the implementation simple
satisfies the metadata documentation requirement
supports feature versioning
enables repeatable training/inference retrieval
is easier to explain in a course demo

The assignment explicitly allows a custom metadata registry as a valid feature store implementation .
Core components
1. Feature tables
Entity-level feature files stored as versioned datasets.
2. Metadata registry
A JSON registry that documents feature definitions.
3. Retrieval interface
Utility functions that load a specified feature version for:

model training
batch scoring
user/item lookup during inference


Feature Registry Schema
Each registered feature should include metadata such as:

feature_name
entity
description
source_table
source_columns
transformation
data_type
version
used_for_training
used_for_inference

Example registry entries


user_interaction_count
Count of interactions grouped by user_id


item_popularity_rank
Rank of items by weighted interaction volume


price_bucket
Bucketized form of product price


content_text_combined
Concatenation of product text fields for content-based modeling


This metadata directly supports the assignment requirement to document feature names, sources, and transformations .

Versioning Strategy
Feature outputs are stored by version.
Example
data/features/v1_2026_04_29/
Each version should capture:

feature build timestamp
source input references
transformation logic version
output file paths

This supports lineage, reproducibility, and consistent train/serve behavior. It also aligns with the assignment’s broader emphasis on data versioning and transformation traceability .

Retrieval Modes
Training retrieval
Used when building recommendation models.
Typical flow:

load a specific feature version
join user, item, and interaction features
generate the model training matrix
log the version used in MLflow or run metadata

Inference retrieval
Used when generating recommendations for a user or item.
Typical flow:

load the requested or latest feature version
fetch features for the target entity
apply the same feature logic used during training
avoid training-serving mismatch

The assignment specifically calls for versioned retrieval for both training and inference .

Fit Within the End-to-End Pipeline
This layer appears after preparation and before model training:
ingestion → raw storage → validation → preparation → feature engineering → feature store → model training → evaluation → orchestration
That sequence mirrors the assignment pipeline expectations for the recommendation system workflow .

Model Support
These features are intended to support three recommendation approaches in the project:
SVD
Uses interaction-level and aggregate user/item signals for collaborative filtering.
Item-Based Collaborative Filtering
Uses item interaction statistics, co-occurrence-friendly data, and popularity signals.
Content-Based Recommendation
Uses product metadata such as title, category, brand, and price-derived features for item similarity.
This is consistent with the assignment requirement to train collaborative and content-based recommendation models and evaluate them using ranking metrics .

Data Notes and Constraints
A practical note from the prepared data: the current prepared outputs contain a very large interaction table but a much smaller product table, with about 2.76M interactions versus 194 prepared products . That matters for content-based recommendation because model coverage depends on overlap between interaction items and items available in the product catalog.
Implication:

collaborative models may have broader behavior coverage
content-based models may require overlap filtering or richer item metadata coverage

That is not a flaw in the feature layer, but it is an important design constraint to document.

Deliverables Covered
This module supports the following assignment deliverables:
Feature Engineering and Transformation

transformation scripts
recommendation-ready features
structured transformed outputs
summary of feature logic

Feature Store

custom feature store implementation
feature metadata documentation
versioned retrieval design
sample training/inference retrieval support


Strengths

simple and modular
easy to demo and explain
reproducible through versioned outputs
supports both collaborative and content-based pipelines
fits the assignment scope without unnecessary platform overhead


Limitations

custom registry is lighter than production-grade tools like Feast
online serving is simulated rather than deployed
feature freshness depends on rebuild cadence
content-based coverage depends on available product metadata overlap

