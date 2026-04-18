# ADR 002: Redshift Serverless over Provisioned

## Status
Accepted

## Context
We need a Redshift endpoint for source view analysis and output verification. Options:
- Redshift Serverless (auto-scaling, pay-per-query)
- Redshift Provisioned (fixed cluster)

## Decision
Use Redshift Serverless for both dev and prod.

## Rationale
1. **Cost** — Auto-pauses when idle, no charges during development downtime
2. **Scaling** — Auto-scales RPU based on query complexity
3. **Operations** — No cluster management, patching, or resize operations
4. **Dev/prod parity** — Same service in both environments

## Consequences
- Minimum billing is 60 seconds per query (fine for ETL validation)
- Some Redshift features (e.g., AQUA) not available on Serverless
- Cold start latency (~30s) when resuming from idle
