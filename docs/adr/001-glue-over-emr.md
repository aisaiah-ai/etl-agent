# ADR 001: AWS Glue over EMR Serverless

## Status
Accepted

## Context
We need a serverless Spark runtime to execute migrated PySpark jobs. Options:
- AWS Glue (managed Spark ETL service)
- EMR Serverless (general-purpose Spark)
- EMR on EC2 (self-managed clusters)

## Decision
Use AWS Glue 4.0 for PySpark job execution.

## Rationale
1. **Native Glue Catalog integration** — Glue jobs read/write to the same catalog that Redshift Spectrum uses, eliminating schema translation
2. **Built-in job management** — Job bookmarks, triggers, and crawlers simplify operations
3. **Simpler deployment** — Single API call to create/update jobs vs EMR application lifecycle
4. **Cost** — Pay per DPU-second with no idle charges, comparable to EMR Serverless
5. **CI/CD friendly** — Script-based deployment (upload to S3 + update job) fits GitHub Actions

## Consequences
- Glue has a ~1 min cold start per job run (acceptable for batch ETL)
- Limited to Spark versions supported by Glue (currently Spark 3.3 in Glue 4.0)
- Glue-specific APIs (GlueContext, DynamicFrame) may be needed for some operations
