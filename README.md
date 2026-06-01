## Project Overview
The pipeline implements an ELT (Extract, Load, Transform) pattern to manage company records. It handles the transition from static historical snapshots (CSV) to live operational data (JSON), storing everything in a PostgreSQL database for advanced analysis.

### Data Sources
* **Bulk Data:** Public snapshots provided by the [Companies House Download Service](https://download.companieshouse.gov.uk/en_output.html).
* **Live Data:** Real-time updates via the [Companies House Public Data API](https://developer.company-information.service.gov.uk/overview).

### Key Features
* **Orchestration:** Managed entirely by Apache Airflow (Webserver, Scheduler, and Init Engine) to schedule, monitor, and execute the data tasks.
* **Isolated Multi-Database Architecture:** Dedicated business data database and a standalone Airflow metadata database within the same PostgreSQL instance.
* **Modular Class Architecture:** Encapsulated logic for CSV and API operations.
* **Memory-Efficient Processing:** Uses Pandas chunking to process large `.zip` files without crashing.
* **Data Enrichment:** Fetches live profiles from the Companies House API to update company statuses.
* **Schema Enforcement:** Uses SQLAlchemy models to ensure data integrity across different sources.
* **Automated Reconciliation:** Generates discrepancy reports identifying mismatches between bulk records and live API data.
* **Dockerized Architecture:** Fully containerized environment including the Python pipeline, PostgreSQL 18, and pgAdmin 4.
* **Idempotent Upsert Logic:** Uses PostgreSQL `ON CONFLICT` logic to ensure data is updated if it exists or inserted if it is new, preventing duplicates.

### Tech Stack
* **Language:** Python 3.10+
* **Database:** PostgreSQL 18
* **Libraries:** SQLAlchemy (ORM), Pandas (Data Processing), HTTPX (API Communication), Python-Dotenv
* **Orchestration:** Docker & Docker Compose
* **Development:** Decoupled architecture for easy testing and scaling.

## Setup & Installation

### 1. Environment & Airflow Initialization
Before building the containers, you must prepare the host environment permissions and create the required Airflow directories (as specified in the Airflow documentation). In your Linux/WSL terminal, run the following official initialization steps:

--> In bash
#### Create necessary mounting directories for Airflow
mkdir -p ./logs ./plugins ./output_airflow

#### Initialize the .env file with your local host UID and GID mapping
echo "AIRFLOW_UID=$(id -u)" > .env
echo "AIRFLOW_GID=0" >> .env

### 2. Environment Configuration
The pipeline requires an API key and database credentials. Locate (or create) the .env file in the root directory. Fill in your Companies House API Key and preferred database credentials, template below and in .env.example:

#### From step 1
AIRFLOW_UID=1000
AIRFLOW_GID=0
#### Business Database Configuration 
DB_USER=your_secure_db_user
DB_PASSWORD=your_secure_db_password
DB_NAME=companies_data_db
DB_HOST=localhost
DB_PORT=5432
AIRFLOW_DB_NAME=airflow_metadata_db
#### Companies House API Configuration 
DB_API_KEY=your_actual_companies_house_api_key
#### Airflow Web UI Admin User Configuration 
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin_secure_password


### 3. Running with Docker (Recommended)
The project is fully containerized. Docker Compose handles the database, GUI, and the Python environment automatically. To build and start all services (in the background), run:

docker-compose up --build -d

#### Running the Pipeline Manually (Without Airflow)
Because the standalone pipeline service is explicitly isolated under a Docker profile, it will remain idle and will not execute automatically alongside Airflow. If you want to bypass the scheduler and trigger the data tasks manually inside a standalone container, execute the following command:

docker-compose run --rm pipeline






### This project is still under development. ####