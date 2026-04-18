#!/usr/bin/env bash
# run_local.sh — Run the ETL pipeline locally using Docker Compose services.
#
# Connects to:
#   - PostgreSQL on localhost:5439 (Redshift stand-in)
#   - LocalStack on localhost:4566 (Glue catalog, S3)
#
# Usage:
#   ./scripts/run_local.sh          # Full pipeline (starts Docker if needed)
#   ./scripts/run_local.sh --skip-docker  # Skip Docker startup (already running)
#   ./scripts/run_local.sh --discover-only  # Only run discovery step
#   ./scripts/run_local.sh --jdbc          # Use JDBC reads instead of Glue catalog

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="${PROJECT_ROOT}/output"

export ETL_LOCAL=1
export REDSHIFT_HOST=localhost
export REDSHIFT_PORT=5439
export REDSHIFT_DATABASE=etl_agent_db
export REDSHIFT_USER=admin
export REDSHIFT_PASSWORD=local_dev_password
export LOCALSTACK_URL=http://localhost:4566
export GLUE_DATABASE=etl_agent_local

SKIP_DOCKER=0
DISCOVER_ONLY=0
SOURCE_READ_MODE=catalog
for arg in "$@"; do
    case "$arg" in
        --skip-docker) SKIP_DOCKER=1 ;;
        --discover-only) DISCOVER_ONLY=1 ;;
        --jdbc) SOURCE_READ_MODE=jdbc ;;
    esac
done

echo "=== ETL Agent — Local Pipeline Run ==="
echo ""

# ── Start Docker services if needed ──────────────────────────────────────
if [ "$SKIP_DOCKER" -eq 0 ]; then
    echo "[0/4] Starting Docker Compose services..."
    cd "$PROJECT_ROOT"
    docker compose up -d --wait 2>/dev/null || docker-compose up -d 2>/dev/null
    echo "  Waiting for PostgreSQL to be ready..."
    for i in $(seq 1 30); do
        if pg_isready -h localhost -p 5439 -U admin -q 2>/dev/null; then
            break
        fi
        sleep 1
    done
    echo "  Services ready."
    echo ""
fi

mkdir -p "${OUTPUT_DIR}"

# ── Step 1: Discover Redshift views and Glue catalog ─────────────────────
echo "[1/4] Discovering schemas from local Redshift (PostgreSQL) and Glue (LocalStack)..."
cd "$PROJECT_ROOT"
python -c "
import json
import pathlib
from pipelines.discovery.schema_discovery import SchemaDiscovery

discovery = SchemaDiscovery.local()

# List all tables and views
all_objects = discovery.discover_redshift_tables()
print(f'  Found {len(all_objects)} database objects:')
for obj in all_objects:
    print(f'    {obj[\"type\"]:20s} {obj[\"schema\"]}.{obj[\"name\"]}')

# Get view definitions
views = discovery.discover_redshift_views()
print(f'  Found {len(views)} views to migrate:')
for v in views:
    print(f'    {v.full_name} ({len(v.columns)} columns)')

# Get Glue catalog tables
glue_tables = discovery.discover_glue_tables('etl_agent_local')
print(f'  Found {len(glue_tables)} Glue catalog tables:')
for t in glue_tables:
    print(f'    {t.full_name} ({len(t.columns)} columns)')

# Find missing tables
missing = discovery.find_missing_tables(views, glue_tables)
if missing:
    print(f'  WARNING: {len(missing)} tables referenced in views but missing from Glue:')
    for m in missing:
        print(f'    - {m}')
else:
    print('  All referenced tables exist in Glue catalog.')

# Save discovery output
output = {
    'database_objects': all_objects,
    'views': [
        {
            'schema': v.schema_name,
            'name': v.view_name,
            'definition': v.definition,
            'columns': [{'name': c.name, 'type': c.data_type, 'nullable': c.is_nullable} for c in v.columns],
        }
        for v in views
    ],
    'glue_tables': [
        {
            'database': t.database,
            'name': t.table_name,
            'columns': [{'name': c.name, 'type': c.data_type} for c in t.columns],
            'location': t.location,
        }
        for t in glue_tables
    ],
    'missing_tables': missing,
}
pathlib.Path('${OUTPUT_DIR}/discovery.json').write_text(json.dumps(output, indent=2))
print('  -> Wrote output/discovery.json')
"

if [ "$DISCOVER_ONLY" -eq 1 ]; then
    echo ""
    echo "=== Discovery Complete (--discover-only) ==="
    exit 0
fi

# ── Step 2: Translate views to PySpark ───────────────────────────────────
echo ""
echo "[2/4] Translating Redshift views to PySpark..."
python -c "
import json
import pathlib
from pipelines.redshift_to_glue.sql_parser import RedshiftSQLParser
from pipelines.redshift_to_glue.translator import SQLToPySparkTranslator
from pipelines.redshift_to_glue.glue_job_generator import GlueJobGenerator

discovery = json.loads(pathlib.Path('${OUTPUT_DIR}/discovery.json').read_text())
parser = RedshiftSQLParser()
translator = SQLToPySparkTranslator(
    source_read_mode='${SOURCE_READ_MODE}',
    connection_name='etl-agent-local-redshift',
    redshift_tmp_dir='s3://etl-agent-data-local/redshift-tmp/',
)
generator = GlueJobGenerator(default_output_bucket='etl-agent-data-local')

translations = []
for view in discovery['views']:
    view_ddl = f\"CREATE OR REPLACE VIEW {view['schema']}.{view['name']} AS {view['definition']}\"
    parsed = parser.parse_view_definition(view_ddl)
    pyspark = translator.translate(parsed)
    source_tables = [t for t in parsed.source_tables]

    glue_script = generator.generate(
        view_name=view['name'],
        translated_spark=pyspark,
        source_tables=source_tables,
        output_path=f\"s3://etl-agent-data-local/output/{view['name']}/\",
    )

    # Save individual Glue job script
    script_path = pathlib.Path('${OUTPUT_DIR}/glue_jobs')
    script_path.mkdir(exist_ok=True)
    (script_path / f\"{view['name']}.py\").write_text(glue_script)

    translations.append({
        'view_name': view['name'],
        'source_tables': source_tables,
        'pyspark_code': pyspark,
        'glue_script_path': str(script_path / f\"{view['name']}.py\"),
    })
    print(f\"  Translated: {view['schema']}.{view['name']} -> output/glue_jobs/{view['name']}.py\")

pathlib.Path('${OUTPUT_DIR}/translations.json').write_text(json.dumps(translations, indent=2))
print(f'  -> Generated {len(translations)} Glue job scripts')
"

# ── Step 3: Run sample query on local Redshift to get baseline ───────────
echo ""
echo "[3/4] Running baseline queries on local Redshift..."
python -c "
import json
import pathlib
from pipelines.discovery.schema_discovery import SchemaDiscovery

discovery_data = json.loads(pathlib.Path('${OUTPUT_DIR}/discovery.json').read_text())
sd = SchemaDiscovery.local()

baselines = {}
for view in discovery_data['views']:
    full_name = f\"{view['schema']}.{view['name']}\"
    try:
        rows = sd.run_query(f'SELECT COUNT(*) FROM {full_name}')
        count = rows[0][0]
        sample = sd.run_query(f'SELECT * FROM {full_name} LIMIT 5')
        baselines[view['name']] = {
            'row_count': count,
            'sample_rows': len(sample),
            'columns': [c['name'] for c in view['columns']],
        }
        print(f\"  {full_name}: {count} rows\")
    except Exception as e:
        print(f\"  {full_name}: ERROR - {e}\")
        baselines[view['name']] = {'error': str(e)}

pathlib.Path('${OUTPUT_DIR}/baselines.json').write_text(json.dumps(baselines, indent=2))
print('  -> Wrote output/baselines.json')
"

# ── Step 4: Verify generated scripts ─────────────────────────────────────
echo ""
echo "[4/4] Verifying generated Glue scripts..."
python -c "
import ast
import pathlib

script_dir = pathlib.Path('${OUTPUT_DIR}/glue_jobs')
if not script_dir.exists():
    print('  No scripts to verify.')
    exit(0)

passed = 0
failed = 0
for script in sorted(script_dir.glob('*.py')):
    try:
        ast.parse(script.read_text())
        print(f'  PASS: {script.name}')
        passed += 1
    except SyntaxError as e:
        print(f'  FAIL: {script.name} — {e}')
        failed += 1

print(f'  Results: {passed} passed, {failed} failed')
"

# ── Summary ──────────────────────────────────────────────────────────────
echo ""
echo "=== Pipeline Complete ==="
echo "Output files:"
find "${OUTPUT_DIR}" -type f | sort | while read -r f; do
    echo "  $(echo "$f" | sed "s|${PROJECT_ROOT}/||")"
done
echo ""
echo "Next steps:"
echo "  - Review generated Glue scripts in output/glue_jobs/"
echo "  - Compare baselines.json with actual Glue job output"
echo "  - Deploy to AWS: ./scripts/deploy.sh dev"
echo "  - Clean up: ./scripts/teardown.sh"
