# ETL Agent — Design Document

## Problem Statement

Organizations running analytical workloads on Amazon Redshift often have complex views built on
external tables sourced from AWS Glue Data Catalog. These views represent business logic that
needs to be replicated as Glue PySpark jobs for:

- Cost optimization (Glue Serverless vs always-on Redshift)
- Data pipeline modernization
- Decoupling compute from storage
- Enabling CI/CD for ETL transformations

Manually converting Redshift SQL to PySpark is error-prone and time-consuming. This platform
automates the migration using AI-powered schema discovery and SQL-to-PySpark translation.

## Architecture

### Pipeline Stages

```
1. DISCOVER    →  Analyze Redshift views, resolve dependencies, identify external tables
2. TRANSLATE   →  Convert Redshift SQL to PySpark transformations
3. DEPLOY      →  Package and deploy as AWS Glue jobs via Step Functions
4. VERIFY      →  Validate output matches Redshift (schema, row counts, samples)
```

### AI Discovery (Bedrock)

The discovery agent uses Amazon Bedrock (Claude) to:
- Analyze complex view DDL and suggest optimal PySpark transformations
- Identify Redshift-specific functions and map to Spark equivalents
- Resolve schema ambiguities in external tables
- Generate dependency-ordered migration plans

### Infrastructure

| Component | Service | Purpose |
|-----------|---------|---------|
| Compute | AWS Glue 4.0 | PySpark job execution |
| Query | Redshift Serverless | Source views and validation |
| Storage | S3 | Data lake (input/output) |
| Catalog | Glue Data Catalog | Schema registry |
| Orchestration | Step Functions | Pipeline execution |
| AI | Amazon Bedrock | Schema discovery |
| Monitoring | CloudWatch | Logs, alarms |
| IaC | Terraform | Infrastructure management |

### Data Flow

```
Redshift View DDL
    │
    ▼
Schema Discovery (Bedrock)
    │ ── identifies source tables, columns, types
    │ ── resolves external table schemas from Glue Catalog
    │ ── detects Redshift-specific functions
    ▼
SQL Parser
    │ ── extracts table references, JOINs, aggregations
    │ ── builds column lineage graph
    ▼
PySpark Translator
    │ ── maps SQL operations to DataFrame API
    │ ── converts Redshift functions to Spark equivalents
    ▼
Glue Job Generator
    │ ── produces complete Glue job scripts
    │ ── configures GlueContext, job parameters
    ▼
Deployer
    │ ── uploads to S3, creates/updates Glue jobs
    ▼
Verifier
    │ ── compares Glue output vs Redshift output
    │ ── schema conformance, row counts, sample data
```

## Local Development

The platform supports fully local development:
- PySpark runs locally (no AWS needed for translation/testing)
- Mock AWS services via moto/localstack for integration tests
- `run_local.sh` executes the full pipeline on sample SQL

## Cost Estimate

| Resource | Monthly Cost (Dev) |
|----------|-------------------|
| Glue Jobs | $0-5 (pay per run) |
| Redshift Serverless | $0-15 (auto-pause) |
| S3 | <$1 |
| Step Functions | <$1 |
| Bedrock (Claude) | $5-20 |
| **Total** | **$10-40** |

## Security

- IAM least-privilege roles per pipeline stage
- KMS encryption for S3
- Redshift credentials via Secrets Manager
- No long-lived AWS keys (OIDC for CI/CD)
