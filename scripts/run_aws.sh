#!/usr/bin/env bash
# run_aws.sh — Run the ETL discovery pipeline against AWS Redshift Serverless
# using the Redshift Data API (no direct connection needed).
#
# Prerequisites:
#   - AWS credentials configured (source docs/aws_mfa_dev.sh)
#   - pip3 install boto3 pyyaml
#
# Usage:
#   ./scripts/run_aws.sh                # Full pipeline
#   ./scripts/run_aws.sh --discover-only  # Only run discovery

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="${PROJECT_ROOT}/output"

export AWS_DEFAULT_REGION="us-east-1"
export REDSHIFT_WORKGROUP="etl-agent-dev-wg"
export REDSHIFT_DATABASE="etl_agent_db"
export GLUE_DATABASE="financial_hist_ext"
export VIEW_SCHEMA="financial_hist"
export ETL_DATA_BUCKET="etl-agent-data-dev"
export ETL_ARTIFACTS_BUCKET="etl-agent-artifacts-dev"

DISCOVER_ONLY=0
SOURCE_READ_MODE=catalog
for arg in "$@"; do
    case "$arg" in
        --discover-only) DISCOVER_ONLY=1 ;;
        --jdbc) SOURCE_READ_MODE=jdbc ;;
    esac
done

echo "=== ETL Agent — AWS Pipeline Run ==="
echo "  Workgroup: $REDSHIFT_WORKGROUP"
echo "  Database:  $REDSHIFT_DATABASE"
echo "  Schema:    $VIEW_SCHEMA"
echo "  Glue DB:   $GLUE_DATABASE"
echo ""

mkdir -p "${OUTPUT_DIR}"

# ── Step 1: Discover Redshift views and Glue catalog ─────────────────────
echo "[1/4] Discovering schemas from Redshift (Data API) and Glue catalog..."
cd "$PROJECT_ROOT"
python3 -c "
import json, time, pathlib, boto3, os

wg = os.environ['REDSHIFT_WORKGROUP']
db = os.environ['REDSHIFT_DATABASE']
glue_db = os.environ['GLUE_DATABASE']
view_schema = os.environ['VIEW_SCHEMA']

rs = boto3.client('redshift-data', region_name='us-east-1')
glue = boto3.client('glue', region_name='us-east-1')

def run_query(sql):
    resp = rs.execute_statement(WorkgroupName=wg, Database=db, Sql=sql)
    stmt_id = resp['Id']
    while True:
        desc = rs.describe_statement(Id=stmt_id)
        st = desc['Status']
        if st in ('SUBMITTED', 'PICKED', 'STARTED'):
            time.sleep(2)
            continue
        if st == 'FINISHED':
            if desc.get('HasResultSet'):
                result = rs.get_statement_result(Id=stmt_id)
                return result['Records'], result['ColumnMetadata']
            return [], []
        raise RuntimeError(f'Query failed ({st}): {desc.get(\"Error\", \"unknown\")}')

# Discover all tables/views in financial_hist
print('  Querying Redshift for database objects...')
rows, _ = run_query(
    \"\"\"SELECT table_schema, table_name, table_type
       FROM information_schema.tables
       WHERE table_schema NOT IN ('pg_catalog','information_schema','pg_internal')
       ORDER BY table_schema, table_name\"\"\"
)
all_objects = []
for row in rows:
    obj = {'schema': row[0].get('stringValue',''), 'name': row[1].get('stringValue',''), 'type': row[2].get('stringValue','')}
    all_objects.append(obj)
    print(f'    {obj[\"type\"]:20s} {obj[\"schema\"]}.{obj[\"name\"]}')
print(f'  Found {len(all_objects)} database objects.')

# Get view definitions
print('  Querying view definitions...')
rows, _ = run_query(
    f\"\"\"SELECT schemaname, viewname, definition
        FROM pg_views
        WHERE schemaname = '{view_schema}'
        ORDER BY viewname\"\"\"
)
views = []
for row in rows:
    v = {
        'schema': row[0].get('stringValue',''),
        'name': row[1].get('stringValue',''),
        'definition': row[2].get('stringValue',''),
        'columns': [],
    }
    views.append(v)
    print(f'    {v[\"schema\"]}.{v[\"name\"]}')
print(f'  Found {len(views)} views to migrate.')

# Get column info for each view
if views:
    view_names = ','.join([\"'\" + v['name'] + \"'\" for v in views])
    rows, _ = run_query(
        f\"\"\"SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{view_schema}' AND table_name IN ({view_names})
            ORDER BY table_name, ordinal_position\"\"\"
    )
    cols_map = {}
    for row in rows:
        tname = row[0].get('stringValue','')
        cols_map.setdefault(tname, []).append({
            'name': row[1].get('stringValue',''),
            'type': row[2].get('stringValue',''),
            'nullable': row[3].get('stringValue','') == 'YES',
        })
    for v in views:
        v['columns'] = cols_map.get(v['name'], [])
        print(f'    {v[\"name\"]}: {len(v[\"columns\"])} columns')

# Get Glue catalog tables
print(f'  Querying Glue catalog: {glue_db}...')
glue_tables = []
paginator = glue.get_paginator('get_tables')
for page in paginator.paginate(DatabaseName=glue_db):
    for tbl in page['TableList']:
        sd = tbl.get('StorageDescriptor', {})
        columns = [{'name': c['Name'], 'type': c['Type']} for c in sd.get('Columns', [])]
        glue_tables.append({
            'database': glue_db,
            'name': tbl['Name'],
            'columns': columns,
            'location': sd.get('Location', ''),
        })
        print(f'    {glue_db}.{tbl[\"Name\"]} ({len(columns)} columns)')
print(f'  Found {len(glue_tables)} Glue catalog tables.')

# Save discovery output
output = {
    'database_objects': all_objects,
    'views': views,
    'glue_tables': glue_tables,
    'missing_tables': [],
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
python3 -c "
import json
import pathlib
from pipelines.redshift_to_glue.sql_parser import RedshiftSQLParser
from pipelines.redshift_to_glue.translator import SQLToPySparkTranslator
from pipelines.redshift_to_glue.glue_job_generator import GlueJobGenerator

discovery = json.loads(pathlib.Path('${OUTPUT_DIR}/discovery.json').read_text())
parser = RedshiftSQLParser()
translator = SQLToPySparkTranslator(
    source_read_mode='${SOURCE_READ_MODE}',
    connection_name='etl-agent-dev-redshift',
    redshift_tmp_dir='s3://${ETL_ARTIFACTS_BUCKET}/redshift-tmp/',
)
generator = GlueJobGenerator(default_output_bucket='${ETL_DATA_BUCKET}')

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
        output_path=f's3://${ETL_DATA_BUCKET}/output/{view[\"name\"]}/',
    )

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

# ── Step 3: Run baseline queries ─────────────────────────────────────────
echo ""
echo "[3/4] Running baseline queries on Redshift (Data API)..."
python3 -c "
import json, time, pathlib, boto3, os

wg = os.environ['REDSHIFT_WORKGROUP']
db = os.environ['REDSHIFT_DATABASE']

rs = boto3.client('redshift-data', region_name='us-east-1')

def run_query(sql):
    resp = rs.execute_statement(WorkgroupName=wg, Database=db, Sql=sql)
    stmt_id = resp['Id']
    while True:
        desc = rs.describe_statement(Id=stmt_id)
        st = desc['Status']
        if st in ('SUBMITTED', 'PICKED', 'STARTED'):
            time.sleep(2)
            continue
        if st == 'FINISHED':
            if desc.get('HasResultSet'):
                result = rs.get_statement_result(Id=stmt_id)
                return result['Records']
            return []
        raise RuntimeError(f'Query failed ({st}): {desc.get(\"Error\", \"unknown\")}')

discovery_data = json.loads(pathlib.Path('${OUTPUT_DIR}/discovery.json').read_text())

baselines = {}
for view in discovery_data['views']:
    full_name = f\"{view['schema']}.{view['name']}\"
    try:
        rows = run_query(f'SELECT COUNT(*) FROM {full_name}')
        count = rows[0][0].get('longValue', 0)
        baselines[view['name']] = {
            'row_count': count,
            'columns': [c['name'] for c in view['columns']],
        }
        print(f'  {full_name}: {count} rows')
    except Exception as e:
        print(f'  {full_name}: ERROR - {e}')
        baselines[view['name']] = {'error': str(e)}

pathlib.Path('${OUTPUT_DIR}/baselines.json').write_text(json.dumps(baselines, indent=2))
print('  -> Wrote output/baselines.json')
"

# ── Step 4: Verify generated scripts ─────────────────────────────────────
echo ""
echo "[4/4] Verifying generated Glue scripts..."
python3 -c "
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

echo ""
echo "=== Pipeline Complete ==="
echo "Output files:"
find "${OUTPUT_DIR}" -type f | sort | while read -r f; do
    echo "  $(echo "$f" | sed "s|${PROJECT_ROOT}/||")"
done
