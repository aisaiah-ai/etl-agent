# ETL Agent — Redshift-to-Glue Migration Platform

Automated ETL platform that migrates Redshift queries and views to AWS Glue PySpark jobs,
with AI-powered schema discovery via Amazon Bedrock.

## Architecture

```
Redshift Views/Queries
        │
        ▼
┌─────────────────┐     ┌──────────────────┐
│  Discovery Agent │────▶│  Schema Registry │
│  (Bedrock/SM)    │     │  (Glue Catalog)  │
└─────────────────┘     └──────────────────┘
        │
        ▼
┌─────────────────┐     ┌──────────────────┐
│  SQL-to-PySpark  │────▶│  Glue PySpark    │
│  Translator      │     │  Jobs            │
└─────────────────┘     └──────────────────┘
        │
        ▼
┌─────────────────────────────────────────┐
│  Step Functions Orchestration            │
│  Discover → Translate → Deploy → Verify  │
└─────────────────────────────────────────┘
```

## Quick Start

```bash
# Setup
make setup
source .venv/bin/activate

# Start local services (PostgreSQL as Redshift + LocalStack for Glue/S3)
make up

# Run full local pipeline (discovers views, translates, generates Glue scripts)
make run

# Or step by step:
make discover            # Only discover schemas
make psql                # Connect to local Redshift (PostgreSQL)
make glue-tables         # List Glue catalog tables
make test                # Unit tests (no Docker needed)
make test-local          # Integration tests against local services

# Deploy to dev
./scripts/deploy.sh dev
```

## Local Development

Docker Compose provides local equivalents of AWS services:

| Service | Local | AWS |
|---------|-------|-----|
| Redshift | PostgreSQL on `localhost:5439` | Redshift Serverless |
| Glue Catalog | LocalStack on `localhost:4566` | AWS Glue Data Catalog |
| S3 | LocalStack on `localhost:4566` | Amazon S3 |

```bash
# Start services (auto-seeds with sample data: 5 patients, 7 claims, 5 views)
make up

# Query local Redshift directly
make psql
# => SELECT * FROM public.vw_patient_claims_summary;

# View Glue catalog tables
make glue-tables

# Run the full pipeline locally
make run
# -> output/discovery.json    (schemas from Redshift + Glue)
# -> output/glue_jobs/*.py    (generated PySpark Glue scripts)
# -> output/baselines.json    (row counts from local Redshift)

# Tear down
make down        # Stop containers (keep data)
make down-clean  # Stop and delete volumes
```

## Pipeline Stages

1. **Discover** — AI agent analyzes Redshift views, resolves external tables from Glue Catalog
2. **Translate** — Converts Redshift SQL to PySpark transformations
3. **Deploy** — Packages and deploys Glue jobs via Step Functions
4. **Verify** — Validates output schema and row counts match Redshift

## Project Structure

```
etl-agent/
├── .github/workflows/      # CI/CD pipelines
├── infra/terraform/         # Infrastructure as Code
├── pipelines/               # Core pipeline code
│   ├── redshift_to_glue/    # SQL-to-PySpark translation
│   ├── discovery/           # Schema & dependency discovery
│   ├── glue_jobs/           # Generated Glue job templates
│   └── publish/             # Deployment & verification
├── agents/                  # AI agents (Bedrock integration)
├── tests/                   # Unit & integration tests
├── scripts/                 # Local dev & deployment scripts
├── config/                  # Environment configurations
└── docs/                    # Architecture docs
```

## Requirements

- Python 3.11+
- Docker + Docker Compose (for local dev)
- Terraform >= 1.5 (for infrastructure deployment)
- AWS CLI v2 (for deployment)
