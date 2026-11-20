# Experiment 07 – Workflow Orchestration using Apache Airflow

## Aim

To create, execute, and inspect an Apache Airflow DAG that orchestrates a simple ETL workflow for online-store sales data.

## 1. Prerequisites

Required software:
- Python 3.9 or compatible Python environment
- Apache Airflow 2.7+ (Airflow 3.x supported)
- Pandas
- Web browser
- Terminal

Activate the Python environment:

```bash
source ~/airflow-lab07/venv/bin/activate
```

Verify installations:

```bash
python --version
airflow version
pip show pandas
```

Set the Airflow home directory:

```bash
export AIRFLOW_HOME=~/airflow
```

## 2. Initialize Airflow

Create the Airflow directory:

```bash
mkdir -p "$AIRFLOW_HOME/dags"
```

Initialize or migrate the Airflow metadata database:

```bash
airflow db migrate
```

## 3. Create the DAG File

Create the DAG file:

```bash
nano "$AIRFLOW_HOME/dags/student_demo_etl.py"
```

Add the following code:

```python
"""
Student Demo ETL Pipeline with Apache Airflow

Example: Online Store Sales Data
Flow: extract_sales -> transform_sales -> load_sales -> send_report
"""

try:
    # Airflow 3.x
    from airflow.sdk import dag, task
except ImportError:
    # Airflow 2.x
    from airflow.decorators import dag, task

from datetime import datetime
import pandas as pd


@dag(
    dag_id="student_demo_etl_pipeline",
    description="Student ETL Demo: Extract -> Transform -> Load sales data",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "students_demo",
        "retries": 1
    },
    tags=["demo", "ETL", "students"]
)
def student_demo_etl_pipeline():

    @task(task_id="extract_sales")
    def extract():
        """Extract simulated raw sales data."""
        print("EXTRACT: Fetching raw sales data...")

        raw_data = [
            {"order_id": 1, "product": "Laptop", "price": 50000, "qty": 1, "city": "Mumbai"},
            {"order_id": 2, "product": "Mouse", "price": 500, "qty": 2, "city": "Delhi"},
            {"order_id": 3, "product": "Keyboard", "price": 1200, "qty": 1, "city": "Mumbai"},
            {"order_id": 4, "product": "Monitor", "price": 12000, "qty": 2, "city": "Pune"},
            {"order_id": 5, "product": "Mouse", "price": 500, "qty": 1, "city": "Chennai"},
            {"order_id": 6, "product": "Laptop", "price": 50000, "qty": 1, "city": "Delhi"}
        ]

        print(f"Extracted {len(raw_data)} raw records")
        return raw_data

    @task(task_id="transform_sales")
    def transform(raw_data: list) -> list:
        """Clean and enrich sales data."""

        print("TRANSFORM: Cleaning and enriching sales data...")

        df = pd.DataFrame(raw_data)

        print(f"Raw rows: {len(df)}")

        # Remove records with missing values
        df = df.dropna(subset=["price", "qty"])

        # Normalize product names
        df["product"] = df["product"].str.strip().str.lower()

        # Calculate total order amount
        df["total_amount"] = df["price"] * df["qty"]

        # Classify orders
        df["order_type"] = df["total_amount"].apply(
            lambda x: "HIGH_VALUE" if x > 10000 else "NORMAL"
        )

        print(f"Clean rows: {len(df)}")
        print(df)

        return df.to_dict(orient="records")

    @task(task_id="load_sales")
    def load(clean_data: list) -> str:
        """Save transformed records to CSV."""

        print("LOAD: Saving transformed records to CSV...")

        df = pd.DataFrame(clean_data)

        output_path = "/tmp/student_sales_clean.csv"

        df.to_csv(output_path, index=False)

        total_revenue = df["total_amount"].sum()
        total_orders = len(df)

        summary = (
            f"Loaded {total_orders} orders, "
            f"Total Revenue = Rs.{total_revenue:,.2f} "
            f"to {output_path}"
        )

        print(summary)

        return summary

    @task(task_id="send_report")
    def report(summary: str):
        """Print the ETL completion report."""

        print(f"Daily ETL Success! {summary}")

    raw = extract()
    clean = transform(raw)
    summary = load(clean)
    report(summary)


student_demo_etl_pipeline()
```

Save and exit.

## 4. Verify the DAG

List the available DAGs:

```bash
airflow dags list
```

Check the DAG for import errors:

```bash
airflow dags list-import-errors
```

Check the DAG details:

```bash
airflow dags show student_demo_etl_pipeline
```

## 5. Start Airflow

For a local standalone installation, run:

```bash
airflow standalone
```

Keep the terminal running.

Airflow will initialize the local environment and display the login credentials.

Open the Airflow UI:

```text
http://localhost:8080
```

Log in using the credentials displayed by the standalone command.

## 6. Trigger the DAG

In the Airflow UI:

1. Locate `student_demo_etl_pipeline`.
2. Enable the DAG if it is paused.
3. Click the Trigger DAG button.
4. Open the DAG details.
5. Inspect the Graph view.
6. Check the status of each task.
7. Open task logs to inspect execution details.

The expected task flow is:

```text
extract_sales
      |
      v
transform_sales
      |
      v
load_sales
      |
      v
send_report
```

## 7. Verify the Output CSV

Open another terminal:

```bash
cat /tmp/student_sales_clean.csv
```

The output should contain the transformed sales records.

Expected columns:

```text
order_id,product,price,qty,city,total_amount,order_type
```

## 8. Practice Task – Modify the DAG

Modify the existing DAG to perform the following operations:

1. Add a `discount` field equal to 5% of `total_amount`.
2. Create a task named `validate_sales` between `transform_sales` and `load_sales`.
3. Change the report task to print the number of `HIGH_VALUE` orders.
4. Run the DAG again and compare the output CSV with the previous run.

### 8.1 Add the Discount Field

Inside the `transform()` function, after calculating `total_amount`, add:

```python
df["discount"] = df["total_amount"] * 0.05
```

### 8.2 Create the Validation Task

Add this task inside the DAG function:

```python
@task(task_id="validate_sales")
def validate_sales(clean_data: list) -> list:
    """Validate transformed sales records."""

    df = pd.DataFrame(clean_data)

    if df.empty:
        raise ValueError("No sales records available")

    if df["total_amount"].isnull().any():
        raise ValueError("Missing total_amount values")

    if (df["total_amount"] < 0).any():
        raise ValueError("Negative order amount found")

    print(f"Validation successful: {len(df)} records")

    return clean_data
```

### 8.3 Modify the Report Task

Replace the existing report task with:

```python
@task(task_id="send_report")
def report(summary: str, clean_data: list):
    """Print the ETL completion report."""

    df = pd.DataFrame(clean_data)

    high_value_count = len(
        df[df["order_type"] == "HIGH_VALUE"]
    )

    print(f"Daily ETL Success! {summary}")
    print(f"Number of HIGH_VALUE orders: {high_value_count}")
```

### 8.4 Update the Task Dependencies

Replace the existing task execution section with:

```python
raw = extract()
clean = transform(raw)
validated = validate_sales(clean)
summary = load(validated)
report(summary, validated)
```

The updated workflow is:

```text
extract_sales
      |
      v
transform_sales
      |
      v
validate_sales
      |
      v
load_sales
      |
      v
send_report
```

## 9. Run the Modified DAG

Save the modified DAG file.

Check for import errors:

```bash
airflow dags list-import-errors
```

Trigger the DAG again using the Airflow UI.

Inspect the task status and logs.

## 10. Compare the CSV Output

Display the updated CSV:

```bash
cat /tmp/student_sales_clean.csv
```

Check the updated columns:

```bash
head -n 1 /tmp/student_sales_clean.csv
```

The updated CSV should include:

```text
discount
```

Compare the previous and updated CSV files if a backup was created before rerunning the DAG.

## 11. Stop Airflow

Stop the standalone Airflow process using:

```text
Ctrl + C
```

---

## Result

A student ETL workflow was defined as an Airflow DAG, triggered and monitored in the Airflow UI, and its transformed sales data was saved to a CSV file.

---

**End of Experiment 07**