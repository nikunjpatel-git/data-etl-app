# Data ETL Pipeline with Airflow and Databricks

This project demonstrates a sample ETL pipeline using containerized Apache Airflow orchestrating jobs on Databricks. The pipeline ingests, transforms, and loads customer churn data into a Delta table on Databricks.

## Features
- **Containerized Airflow**: Easily deploy and manage Airflow using Docker Compose.
- **Databricks Integration**: Run Spark Python tasks on Databricks for scalable data processing.
- **Delta Lake**: Store processed data in a Delta table for efficient querying and analytics.

---

## Setup Instructions

### 1. Clone the Repository
```sh
git clone <repo-url>
cd data-etl-app
```

### 2. Create Delta Table in Databricks
Run the following SQL in your Databricks workspace to create the target Delta table:

```sql
CREATE TABLE customer_churn_data (
	CustomerID int,
	Age int,
	Gender string,
	Tenure int,
	MonthlyCharges double,
	ContractType string,
	InternetService string,
	TotalCharges double,
	TechSupport string,
	Churn string,
	date_hour string
)
USING DELTA
CLUSTER BY (date_hour)
```

> **Note:** The `date_hour` column is included to ensure idempotency in the ETL process. This design allows each Airflow run to overwrite data corresponding to a specific hour, so that re-running a task for the same time period will safely update the relevant records without creating duplicates.

### 3. Upload Python File to Databricks Workspace
Upload your ETL script (e.g., `transform_customer_churn.py`) to Databricks:
- Path: `dbfs:/Workspace/Users/<your-email>/transform_customer_churn.py`

Example Airflow Databricks operator config:
```json
"spark_python_task": {
	"python_file": "dbfs:/Workspace/Users/<your-email>/transform_customer_churn.py",
	"parameters": [
		"/Workspace/Users/<your-email>/customer_churn_data.csv",
		"{{ execution_date.strftime('%Y%m%d%H') }}"
	]
}
```

### 4. Configure Environment Variables
- Copy `.env.example` to `.env` and update with your credentials.
- Generate a Fernet key for Airflow metadata encryption:

```sh
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```
- Add the generated key to your `.env` file.

### 5. Build and Start Services
```sh
docker-compose build
docker-compose up -d
```

### 6. Initialize Airflow Database
```sh
docker-compose run airflow-init
```

### 7. Create Airflow Admin User
```sh
docker-compose run airflow-webserver airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.org
```
- When prompted, enter a password for the admin user.

### 8. Access Airflow UI
- Open: [https://localhost:8080](https://localhost:8080)

### 9. Configure Databricks Connection in Airflow UI
- **Host**: `https://<your-databricks-instance>.databricks.com`
- **Password**: Your Databricks personal access token

### 10. Unpause the Airflow DAG
- In the Airflow UI, unpause the DAG to start scheduling.

### 11. Create Dashboards
- Use Databricks Dashboards to visualize data from the Unity Catalog tables.

### 12. Restarting Containers
If you restart containers, re-initialize Airflow DB:
```sh
docker compose restart
docker-compose run airflow-init
```

---

## Notes
- Ensure all credentials and sensitive information are kept secure.
- Refer to `.env.example` for required environment variables.

---

## License
MIT License