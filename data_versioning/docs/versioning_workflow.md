# Data Versioning and Lineage Workflow

## Objective
Version raw and transformed datasets, track lineage metadata, and document how datasets move through the RecoMart pipeline.

## Recommended Workflow
1. Ingest source data into partitioned raw folders under data/raw using source and load timestamp conventions.
2. Store standardized outputs in bronze and downstream prepared/transformed/features locations.
3. Version datasets with DVC where available, or maintain this custom registry with hashes and metadata when DVC pointers are absent.
4. Record metadata including dataset name, path, source system, ingestion date, stage, transformations, and file hash.
5. Preserve validation reports, fix logs, and revalidation summaries in reports/validation for auditability.
6. Regenerate lineage_registry artifacts after each ingestion or transformation run.

## Project Observations
- Datasets discovered: 32
- Versioning assets discovered: 7
- DVC pointer files discovered: 3
- Git HEAD ref: refs/heads/main
- Git commit hash: 5ce2fe6dbb26109f4dfdc645cb4392aff8253e64

## Metadata Fields Captured
- dataset_name
- relative_path
- stage
- source_system
- file_format
- size_kb
- last_modified
- load_date / load_hour inferred from partitioned paths when present
- applied_transformations
- versioning_tool
- version_label
- dvc_pointer_file
- sha256

## Usage Notes
- Prefer DVC for large dataset versioning and reproducible pulls/checkouts.
- Use Git for code, metadata, and pointer files rather than large raw binaries.
- If Git LFS is present, restrict it to large binary artifacts that do not fit normal Git workflows.
- Keep lineage registry generation as part of the pipeline so metadata stays current.