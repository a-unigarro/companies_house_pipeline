## Project Overview
The pipeline implements an ELT (Extract, Load, Transform) pattern to manage company records. It handles the transition from static historical snapshots (CSV) to live operational data (JSON), storing everything in a PostgreSQL database for advanced analysis.

### Data Sources
Bulk Data:*Public snapshots provided by the [Companies House Download Service](https://download.companieshouse.gov.uk/en_output.html).
Live Data:*Real-time updates via the [Companies House Public Data API](https://developer.company-information.service.gov.uk/overview).

### Key Features
Modular Class Architecture: Encapsulated logic for CSV and API operations.
Memory-Efficient Processing: Uses Pandas chunking to process large `.zip` files without crashing.
Data Enrichment: Fetches live profiles from the Companies House API to update company statuses.
Schema Enforcement: Uses SQLAlchemy models to ensure data integrity across different sources.
Automated Reconciliation: Generates discrepancy reports identifying mismatches between bulk records and live API data.
Dockerized Architecture: Fully containerized environment including the Python pipeline, PostgreSQL 18, and pgAdmin 4.
Idempotent Upsert Logic: Uses PostgreSQL `ON CONFLICT` logic to ensure data is updated if it exists or inserted if it is new, preventing duplicates.

## Tech Stack
Language: Python 3.10+
Database: PostgreSQL
Libraries: SQLAlchemy (ORM), Pandas (Data Processing), HTTPX (API Communication), Dotenv
Orchestration: Docker & Docker Compose
Development: Decoupled architecture for easy testing and scaling.

## Setup & Installation

### 1. Environment Configuration
The pipeline requires an API key and database credentials. 
1. Locate the `.env.example` file in the root directory.
2. Rename it to `.env`.
3. Fill in your **Companies House API Key** and preferred database credentials.

### 2. Running with Docker (Recommended)
The project is fully containerized. Docker Compose handles the database, GUI, and the Python environment automatically.
# Build and start all services
docker-compose up --build


### This project is still under development. ####