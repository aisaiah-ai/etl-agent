# ETL Agent

## Project Overview
Redshift-to-Glue migration platform with AI-powered schema discovery via Amazon Bedrock.
Converts Redshift views/queries into AWS Glue PySpark jobs, deployed via Step Functions.

## Tech Stack
- Python 3.11+, PySpark 3.5+
- AWS: Glue 4.0, Redshift Serverless, Step Functions, S3, Bedrock, CloudWatch
- IaC: Terraform >= 1.5
- CI/CD: GitHub Actions (4 workflows)
- Testing: pytest, moto, localstack

## Key Commands
```bash
make up                        # Start Docker (PostgreSQL + LocalStack)
make run                       # Full local pipeline
make discover                  # Discovery only
make test                      # Unit tests (no Docker needed)
make test-local                # Integration tests (needs Docker)
make psql                      # Connect to local Redshift
make glue-tables               # List Glue catalog tables
./scripts/deploy.sh dev        # Deploy to dev
ruff check .                   # Lint
black --check .                # Format check
```

### SF→Moodle Sync Pipeline
```bash
# Seed / reset Moodle sandbox test data
bash scripts/seed_moodle.sh --status    # Show current Moodle state
bash scripts/seed_moodle.sh --reset     # Reset to original seed values
bash scripts/seed_moodle.sh --seed      # Create users+courses (first time)

# Deploy + run production sync pipeline (needs: source docs/aws_mfa_prod.sh)
bash scripts/deploy_sf_moodle_sync.sh            # Audit only (read-only)
bash scripts/deploy_sf_moodle_sync.sh sync        # Sync all (courses+users)
bash scripts/deploy_sf_moodle_sync.sh sync --force              # Overwrite conflicts
bash scripts/deploy_sf_moodle_sync.sh sync --entity users       # Users only (students+faculty)
bash scripts/deploy_sf_moodle_sync.sh sync --entity courses     # Courses only

# Validate sync results (before/after report with pass/fail)
bash scripts/validate_sync.sh                     # Full: reset → sync → report
bash scripts/validate_sync.sh --check-only        # Just check current state

# Cleanup
bash scripts/seed_moodle.sh --cleanup                 # Delete test data from Moodle
bash scripts/cleanup_glue_test_jobs.sh                # Dry run — list test jobs
bash scripts/cleanup_glue_test_jobs.sh --delete       # Delete old test Glue jobs

# Local sync (no Glue, uses JSON files)
MOODLE_URL=https://ies-sbox.unhosting.site MOODLE_TOKEN=... \
  python3 -m pipelines.sf_moodle_sync.verify_sync audit users --sf-accounts output/sf_accounts.json
```

## Local Development
- `ETL_LOCAL=1` env var switches to local mode (PostgreSQL + LocalStack)
- PostgreSQL on localhost:5439 stands in for Redshift (wire-compatible)
- LocalStack on localhost:4566 provides Glue catalog and S3
- `scripts/seed_redshift.sql` creates tables, sample data, and views on startup
- `scripts/seed_localstack.sh` creates Glue database and tables on startup

## Project Structure
- `pipelines/redshift_to_glue/` — SQL parsing and PySpark translation
- `pipelines/discovery/` — Schema discovery (Redshift + Glue catalog + Bedrock AI)
- `pipelines/sf_moodle_sync/` — SF→Moodle ID sync pipeline
  - `user_sync.py` — Student sync (SF Account → Moodle user idnumber)
  - `course_sync.py` — Course sync (SF CourseOffering → Moodle course idnumber)
  - `user_sync.py` — User sync (SF Account email → Moodle user idnumber, students+faculty)
  - `verify_sync.py` — Audit/sync/verify CLI
  - `moodle_client.py` — Moodle REST API client
  - `glue_jobs/sf_moodle_sync_job.py` — Production Glue job (courses + users)
- `pipelines/glue_jobs/` — Glue job deployment
- `pipelines/publish/` — Output verification
- `agents/` — Bedrock agent wrapper
- `infra/terraform/` — Infrastructure (modules + dev/prod envs)
- `config/settings.yaml` — Runtime configuration

## Conventions
- Ruff + Black for code style (line-length 100)
- Terraform modules in `infra/terraform/modules/`, environments in `envs/`
- Tests mirror source structure in `tests/unit/`
- No secrets in code — use env vars or Secrets Manager
