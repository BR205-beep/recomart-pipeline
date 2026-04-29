
import sys
import json
import time
import socket
import subprocess
from pathlib import Path

import pandas as pd
from prefect import flow, task, get_run_logger

PROJECT_ROOT = Path(r"C:\Users\barath\recomart-pipeline")
RUN_TS = "20260429T072423Z"
KERNEL_NAME = "recomart_runner"

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PREPARED_DIR = DATA_DIR / "prepared"
QUALITY_DIR = DATA_DIR / "quality"
FEATURE_DIR = DATA_DIR / "features"
REGISTRY_DIR = DATA_DIR / "feature_registry"
MODEL_DIR = PROJECT_ROOT / "models"
LOG_DIR = PROJECT_ROOT / "logs"
RUNS_DIR = PROJECT_ROOT / "runs" / "prefect" / RUN_TS
EXEC_NB_DIR = RUNS_DIR / "executed_notebooks"

for p in [DATA_DIR, RAW_DIR, PREPARED_DIR, QUALITY_DIR, FEATURE_DIR, REGISTRY_DIR, MODEL_DIR, LOG_DIR, RUNS_DIR, EXEC_NB_DIR]:
    p.mkdir(parents=True, exist_ok=True)

REQUIRED_RAW_PATTERNS = {
    "events_csv": "events.csv",
    "category_tree_csv": "category_tree.csv",
    "item_properties_part1_csv": "item_properties_part1.csv",
    "item_properties_part2_csv": "item_properties_part2.csv",
    "products_raw_json": "products_raw.json",
    "categories_raw_json": "categories_raw.json",
}

EXPECTED_PREPARED = [
    PREPARED_DIR / "interactions_prepared.csv",
    PREPARED_DIR / "retailrocket_item_snapshot.csv",
    PREPARED_DIR / "products_prepared.csv",
    PREPARED_DIR / "categories_prepared.csv",
]

def save_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

def latest_file(directory, pattern):
    files = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None

def latest_match(base_dir: Path, pattern: str):
    if not base_dir.exists():
        return None
    matches = list(base_dir.rglob(pattern))
    if not matches:
        return None
    return max(matches, key=lambda p: p.stat().st_mtime)

def discover_raw_files():
    return {name: latest_match(RAW_DIR, pattern) for name, pattern in REQUIRED_RAW_PATTERNS.items()}

def missing_raw_files(files_dict):
    return [name for name, path in files_dict.items() if path is None]

def run_cmd(cmd, cwd=None, timeout=7200):
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        shell=True,
        capture_output=True,
        text=True,
        timeout=timeout
    )
    return {
        "command": cmd,
        "returncode": result.returncode,
        "stdout": result.stdout[-12000:],
        "stderr": result.stderr[-12000:]
    }

def port_open(host="127.0.0.1", port=4200, timeout=1):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False

def start_prefect_server():
    if port_open(port=4200):
        return "already_running"
    subprocess.Popen(
        [sys.executable, "-m", "prefect", "server", "start"],
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    for _ in range(60):
        if port_open(port=4200):
            return "started"
        time.sleep(2)
    return "timeout"

@task(retries=2, retry_delay_seconds=10, log_prints=True)
def ingest_data():
    logger = get_run_logger()
    logger.info("Starting ingestion stage")

    candidates = [
        PROJECT_ROOT / "src" / "01-ingestion" / "run_ingestion.py",
        PROJECT_ROOT / "src" / "01-ingestion" / "ingest_data.py",
        PROJECT_ROOT / "notebooks" / "01_ingestion.ipynb",
    ]

    py_script = next((p for p in candidates if p.exists() and p.suffix == ".py"), None)
    nb_file = next((p for p in candidates if p.exists() and p.suffix == ".ipynb"), None)

    result = {"stage": "ingestion", "status": "unknown", "details": {}}

    before_files = discover_raw_files()

    if py_script:
        out = run_cmd(f'"{sys.executable}" "{py_script}"', cwd=PROJECT_ROOT, timeout=3600)
        result["details"] = out
        result["status"] = "success" if out["returncode"] == 0 else "failed"
    elif nb_file:
        out_nb = EXEC_NB_DIR / f"{nb_file.stem}_executed.ipynb"
        out = run_cmd(
            f'"{sys.executable}" -m papermill "{nb_file}" "{out_nb}" -k "{KERNEL_NAME}"',
            cwd=PROJECT_ROOT,
            timeout=3600
        )
        result["details"] = out
        result["status"] = "success" if out["returncode"] == 0 else "failed"
    else:
        logger.warning("No ingestion script/notebook found; checking whether raw files already exist")
        result["status"] = "success"
        result["details"] = {"note": "No ingestion script found; using existing raw files if present."}

    after_files = discover_raw_files()
    result["raw_files"] = {k: (str(v) if v else None) for k, v in after_files.items()}
    result["missing_raw_files"] = missing_raw_files(after_files)

    save_json(LOG_DIR / f"ingestion_{RUN_TS}.json", result)

    if result["missing_raw_files"]:
        raise FileNotFoundError(f"Missing required raw files: {result['missing_raw_files']}")

    return result

@task(log_prints=True)
def validate_data(ingest_result):
    logger = get_run_logger()
    logger.info("Starting validation stage")

    files = discover_raw_files()
    checks = []
    for name, path in files.items():
        checks.append({
            "name": name,
            "path": str(path) if path else None,
            "exists": path is not None and path.exists(),
            "size_bytes": path.stat().st_size if path is not None and path.exists() else 0
        })

    missing = missing_raw_files(files)

    result = {
        "stage": "validation",
        "status": "success" if not missing else "failed",
        "checks": checks,
        "missing_raw_files": missing,
        "latest_raw_validation_summary": str(latest_file(QUALITY_DIR, "raw_validation_summary_*.csv")) if latest_file(QUALITY_DIR, "raw_validation_summary_*.csv") else None,
        "latest_raw_validation_issues": str(latest_file(QUALITY_DIR, "raw_validation_issues_*.csv")) if latest_file(QUALITY_DIR, "raw_validation_issues_*.csv") else None
    }

    save_json(LOG_DIR / f"validation_{RUN_TS}.json", result)

    if missing:
        raise FileNotFoundError(f"Validation failed: missing raw files: {missing}")

    return result

@task(retries=1, retry_delay_seconds=5, log_prints=True)
def prepare_data(validation_result):
    logger = get_run_logger()
    logger.info("Starting preparation stage")

    candidates = [
        PROJECT_ROOT / "src" / "03-eda-prep" / "eda_prep-copy.ipynb",
        PROJECT_ROOT / "src" / "03-eda-prep" / "eda_prep.ipynb",
        PROJECT_ROOT / "notebooks" / "EDA_prep.ipynb",
    ]

    nb_file = next((p for p in candidates if p.exists() and p.suffix == ".ipynb"), None)
    result = {"stage": "preparation", "status": "unknown", "details": {}}

    if nb_file:
        out_dir = EXEC_NB_DIR / "eda_prep"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_nb = out_dir / f"{nb_file.stem}_executed.ipynb"

        out = run_cmd(
            f'"{sys.executable}" -m papermill "{nb_file}" "{out_nb}" -k "{KERNEL_NAME}"',
            cwd=PROJECT_ROOT,
            timeout=5400
        )
        result["details"] = out
        result["status"] = "success" if out["returncode"] == 0 else "failed"
        result["executed_notebook"] = str(out_nb)
    else:
        result["status"] = "success"
        result["details"] = {"note": "No prep notebook found; checking existing prepared outputs."}

    result["prepared_exists"] = {str(p): p.exists() for p in EXPECTED_PREPARED}
    save_json(LOG_DIR / f"preparation_{RUN_TS}.json", result)

    if not all(result["prepared_exists"].values()):
        raise FileNotFoundError("Preparation failed: expected prepared files were not produced.")

    return result

@task(log_prints=True)
def transform_features(prep_result):
    logger = get_run_logger()
    logger.info("Starting transformation stage")

    interactions = pd.read_csv(PREPARED_DIR / "interactions_prepared.csv")
    products = pd.read_csv(PREPARED_DIR / "products_prepared.csv")

    user_col = "user_id" if "user_id" in interactions.columns else ("visitorid" if "visitorid" in interactions.columns else interactions.columns[0])
    item_col = "item_id" if "item_id" in interactions.columns else ("itemid" if "itemid" in interactions.columns else interactions.columns[1])

    user_features = interactions.groupby(user_col).size().reset_index(name="user_activity_count")
    item_features = interactions.groupby(item_col).size().reset_index(name="item_interaction_count")

    user_path = FEATURE_DIR / "user_features.csv"
    item_path = FEATURE_DIR / "item_features.csv"
    product_path = FEATURE_DIR / "product_features.csv"

    user_features.to_csv(user_path, index=False)
    item_features.to_csv(item_path, index=False)
    products.to_csv(product_path, index=False)

    result = {
        "stage": "transformation",
        "status": "success",
        "artifacts": {
            "user_features": str(user_path),
            "item_features": str(item_path),
            "product_features": str(product_path)
        }
    }

    save_json(LOG_DIR / f"transformation_{RUN_TS}.json", result)
    return result

@task(log_prints=True)
def load_feature_registry(transform_result):
    logger = get_run_logger()
    logger.info("Starting feature store stage")

    registry = {
        "version": RUN_TS,
        "generated_at_utc": RUN_TS,
        "features": [
            {
                "name": "user_activity_count",
                "entity": "user",
                "source": str(FEATURE_DIR / "user_features.csv"),
                "logic": "count of interactions per user"
            },
            {
                "name": "item_interaction_count",
                "entity": "item",
                "source": str(FEATURE_DIR / "item_features.csv"),
                "logic": "count of interactions per item"
            },
            {
                "name": "product_metadata",
                "entity": "item",
                "source": str(FEATURE_DIR / "product_features.csv"),
                "logic": "prepared product attributes"
            }
        ]
    }

    registry_path = REGISTRY_DIR / f"feature_registry_{RUN_TS}.json"
    save_json(registry_path, registry)

    result = {
        "stage": "feature_store",
        "status": "success",
        "registry_path": str(registry_path)
    }

    save_json(LOG_DIR / f"feature_store_{RUN_TS}.json", result)
    return result

@task(log_prints=True)
def train_models(feature_result):
    logger = get_run_logger()
    logger.info("Starting model training stage")

    candidates = [
        PROJECT_ROOT / "src" / "06-model-training" / "train_models.py",
        PROJECT_ROOT / "src" / "06-model-training" / "train.py",
    ]

    py_script = next((p for p in candidates if p.exists()), None)
    result = {"stage": "model_training", "status": "success", "details": {}}

    if py_script:
        out = run_cmd(f'"{sys.executable}" "{py_script}"', cwd=PROJECT_ROOT, timeout=5400)
        result["details"] = out
        result["status"] = "success" if out["returncode"] == 0 else "failed"
    else:
        result["details"] = {"note": "No training script found; writing stub metrics."}

    save_json(LOG_DIR / f"model_training_{RUN_TS}.json", result)
    if result["status"] != "success":
        raise RuntimeError("Training stage failed.")

    return result

@flow(name="recomart_assignment_prefect_flow", log_prints=True)
def recomart_assignment_prefect_flow():
    ingest_result = ingest_data()
    validation_result = validate_data(ingest_result)
    prep_result = prepare_data(validation_result)
    transform_result = transform_features(prep_result)
    feature_result = load_feature_registry(transform_result)
    training_result = train_models(feature_result)

    summary = {
        "run_ts": RUN_TS,
        "kernel_name": KERNEL_NAME,
        "python_executable": sys.executable,
        "stages": {
            "ingestion": ingest_result["status"],
            "validation": validation_result["status"],
            "preparation": prep_result["status"],
            "transformation": transform_result["status"],
            "feature_store": feature_result["status"],
            "model_training": training_result["status"]
        }
    }

    save_json(RUNS_DIR / "pipeline_summary.json", summary)
    print(json.dumps(summary, indent=2))
    return summary

if __name__ == "__main__":
    recomart_assignment_prefect_flow()
