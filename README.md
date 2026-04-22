# Companies House Data Pipeline

Pipeline designed to ingest, enrich, and analyze UK Companies House data. This project handle bulk datasets stored in csv files while synchronizing with live REST APIs to detect data drift.

## Project Overview
The pipeline implements an ELT (Extract, Load, Transform) pattern to manage company records. It handles the transition from static historical snapshots (CSV) to live operational data (JSON), storing everything in a PostgreSQL database for advanced analysis.

### Data Sources
Bulk Data:*Public snapshots provided by the [Companies House Download Service](https://download.companieshouse.gov.uk/en_output.html).
Live Data:*Real-time updates via the [Companies House Public Data API](https://developer.company-information.service.gov.uk/overview).

### Key Features
Modular Class Architecture:*Encapsulated logic for CSV and API operations.
Memory-Efficient Processing:*Uses Pandas chunking to process large `.zip` files without crashing.
Data Enrichment:*Fetches live profiles from the Companies House API to update company statuses.
Schema Enforcement:*Uses SQLAlchemy models to ensure data integrity across different sources.
Automated Reconciliation:*Generates discrepancy reports identifying mismatches between bulk records and live API data.

## Tech Stack
Language:*Python 3.10+
Database:*PostgreSQL
Libraries:*SQLAlchemy (ORM), Pandas (Data Processing), HTTPX (API Communication), Dotenv
Development:*Decoupled architecture for easy testing and scaling.

## Databases