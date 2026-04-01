# PaySim Transaction ETL Pipeline

This project implements a professional ETL (Extract, Transform, Load) pipeline for financial transaction data using **PySpark**, **onETL**, and **FastAPI**. It automates the entire lifecycle of data—from fetching raw datasets from Kaggle to performing high-performance analytical transformations and loading them into a PostgreSQL production environment, with full support for **Kubernetes** orchestration.

## Features
* **Incremental Data Loading**: Implements High Water Mark (HWM) tracking via `onetl` and `YAMLHWMStore` to ensure only new data (based on the `step` column) is processed in subsequent runs.
* **Atomic Transactions**: The HWM state is updated only after a successful load to prevent data gaps or duplicates.
* **Data Quality Gates**: Automated filtering of invalid merchant records, negative balances, and inconsistent transaction logic using Spark transformations.
* **REST API Control**: Integrated FastAPI service to trigger and monitor ETL tasks asynchronously with full history tracking.
* **Cloud-Native Ready**: Fully containerized with Kubernetes manifests for running Spark jobs in a cluster environment.
* **Centralized Configuration**: All Spark settings, database credentials, and HWM parameters are managed via a YAML file and environment variables.

## Project Structure
* `src/`: Core logic containing `etl_pipeline.py` (orchestration) and `transformations.py` (Spark logic).
* `k8s/`: Kubernetes manifests including `secrets.yaml`, `configmap.yaml`, and `spark-job.yaml`.
* `scripts/`: Utility scripts like `load_kaggle_data.py` for initial data ingestion.
* `data/hwm/`: Persistent storage for YAML-based HWM checkpoints.
* `jars/`: Directory for the PostgreSQL JDBC driver.
* `app.py`: FastAPI application for REST API access.
* `config.yaml`: Centralized configuration for Spark, Database, and Kaggle settings.
* `Dockerfile`: Multi-stage build for Spark and Python environment.

## Requirements
* **Python**: 3.10+
* **Java**: OpenJDK 17 (Required for PySpark).
* **Database**: PostgreSQL 16.
* **Infrastructure**: Docker Desktop (with Kubernetes enabled) or Minikube.
* **Environment**: Kaggle API credentials.

---

## Installation

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configure Environment**:
    Create a `.env` file based on `.env.example`:
    ```bash
    #Kaggle configuration
    KAGGLE_USERNAME="your_kaggle_username_here"
    KAGGLE_KEY="your_api_key_here"

    #Database configuration
    DB_PASSWORD="your_database_password_here"
    ```
3.  **Adjust Pipeline Settings**:
    Modify `config.yaml` to set your database host (use `host.docker.internal` for local K8s), table names, and Spark row limits.

---

## Usage

### 1. Source Data Ingestion (`load_kaggle_data.py`)
This script downloads the PaySim dataset from Kaggle, handles schema mapping, and loads it into the source PostgreSQL table.

* **Initial Full Load**:
    ```bash
    python scripts/load_kaggle_data.py
    ```
* **Incremental Testing (Append Mode)**:
    ```bash
    python scripts/load_kaggle_data.py --append
    ```

### 2. Web Service (API)
Start the FastAPI server to manage ETL tasks via REST endpoints:
```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

| Endpoint                | Method | Description |
|:------------------------| :--- | :--- |
| `/etl/full`             | `POST` | Triggers a full snapshot ETL task. |
| `/etl/incremental`      | `POST` | Triggers an incremental load using HWM. |
| `/etl/status/{task_id}` | `GET` | Returns status (pending, running, completed) of a specific task. |
| `/etl/history`          | `GET` | Retrieves the complete history of all ETL runs and their metrics. |

### 3. Kubernetes Deployment
To run the ETL pipeline as a scheduled or manual job in a Kubernetes cluster:

1. **Build Image**:
   ```bash
   docker build -t paysim-app:latest .
   ```
2. **Apply Manifests**:
   ```bash
   kubectl apply -f k8s/secrets.yaml
   kubectl apply -f k8s/configmap.yaml
   kubectl apply -f k8s/spark-job.yaml
   ```
3. **Monitor Logs**:
   ```bash
   kubectl logs -l job-name=spark-trans-job
   ```

---

## Spark Transformations
The pipeline applies complex business logic during the "Transform" stage across three phases:

1.  **Data Cleansing (`filter_invalid_values`)**: 
    * Validates merchant destination data.
    * Ensures all balances and transaction amounts are non-negative.
    * Checks consistency between `amount` and balance changes.
    * Verifies that fraud flags are strictly binary (0 or 1).

2.  **Calculated Fields (`add_calculated_fields`)**: 
    * Calculates `balance_change_orig` and `balance_change_dest`.
    * Derives `transaction_hour` and `transaction_day` from the `step` column for temporal analysis.

3.  **Aggregations (`aggregate_transactions`)**: 
    * Groups data by transaction `type`.
    * Calculates total transaction counts, sum of amounts, and total fraud counts.
    * Computes `fraud_percentage` and sorts the output by the highest fraud probability.

---

## Monitoring
When running in Kubernetes or locally, the **Spark UI** is available at `http://localhost:4040`. If running in K8s, use port-forwarding:
```bash
kubectl port-forward <pod-name> 4040:4040
```