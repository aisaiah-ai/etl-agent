"""
Glue PySpark job: Materialize financial_hist views + historic_financial_qsreport

Reads raw source tables from the Glue catalog (financial_hist_ext),
builds 9 intermediate materialized views via Spark SQL, then runs
the big 9-table LEFT JOIN to produce historic_financial_qsreport.

All intermediate views and the final table are written as Parquet to S3
and registered in the Glue catalog.

Arguments:
  --glue_database    Glue catalog database (default: financial_hist_ext)
  --output_bucket    S3 bucket for output
  --output_prefix    S3 prefix for output tables
"""

import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "glue_database",
    "output_bucket",
    "output_prefix",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.caseSensitive", "false")

GLUE_DB = args["glue_database"]
OUTPUT_BUCKET = args["output_bucket"]
OUTPUT_PREFIX = args["output_prefix"]

print(f"=== Materialize financial_hist views ===")
print(f"  Glue DB: {GLUE_DB}")
print(f"  Output:  s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}/")

# ============================================================================
# Step 1: Load all raw source tables from Glue catalog
# ============================================================================
RAW_TABLES = [
    # Chronus core
    "chronus_gl_transaction_detail",
    "chronus_great_plains_db",
    "chronus_gl_budget_dept",
    "chronus_gl_natural_account",
    "chronus_gl_program",
    "chronus_gl_project",
    "chronus_gl_term",
    "chronus_fiscal_year",
    "chronus_center",
    "chronus_center_program_term",
    "chronus_person",
    "chronus_student_term_admit",
    "chronus_term",
    "chronus_program",
    # Plutus
    "plutus_gl_account_category",
    "plutus_budget_entry",
    "plutus_budget_revision",
    "plutus_budget_revision_currency_fx",
    "plutus_budget_enrollment",
    "plutus_budget_entry_proration",
    "plutus_department_budget",
    # SAF
    "saf_chronus_student_term_admit",
    "saf_chronus_person",
    "saf_chronus_center_program_term",
    "saf_chronus_term",
    "saf_chronus_fiscal_year",
    # Pre-built (pass-through)
    "closed_gl_budget_transactions",
    "historic_all_financial_transactions",
    "fx_rates_hist",
]

# Tables stored as Delta Lake in S3 — read directly to get proper schema from Parquet
# (prd Glue catalog has wrong InputFormat or schema for these)
PRD_DELTA_TABLES = {
    "chronus_gl_program":         "s3://ies-devteam-prd/gluecatalog/prd_glue_chronus_gl_program/",
    "chronus_great_plains_db":    "s3://ies-devteam-prd/gluecatalog/prd_glue_chronus_great_plains_db/",
    "chronus_student_term_admit": "s3://ies-devteam-prd/gluecatalog/prd_glue_chronus_student_term_admit/",
    "plutus_budget_revision":     "s3://ies-devteam-prd/gluecatalog/prd_glue_plutus_budget_revision/",
    "plutus_budget_entry":        "s3://ies-devteam-prd/gluecatalog/prd_glue_plutus_budget_entry/",
    "plutus_budget_enrollment":   "s3://ies-devteam-prd/gluecatalog/prd_glue_plutus_budget_enrollment/",
}

# No data exists anywhere for this table — fx_book_rates view will be empty
SKIP_TABLES = {"plutus_budget_revision_currency_fx"}

print("\n--- Loading raw tables ---")
for tbl in RAW_TABLES:
    print(f"  Loading: {tbl}")

    if tbl in SKIP_TABLES:
        # Create empty DataFrame with explicit schema (catalog has no data → schema lost)
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
        empty_schemas = {
            "plutus_budget_revision_currency_fx": StructType([
                StructField("Budget_Revision_Id", IntegerType(), True),
                StructField("CURRENCY_CODE", StringType(), True),
                StructField("Currency_Per_USD", DoubleType(), True),
            ]),
        }
        schema = empty_schemas.get(tbl, StructType())
        df = spark.createDataFrame([], schema)
        df.createOrReplaceTempView(tbl)
        print(f"    SKIP (no S3 data, empty view with {len(df.columns)} cols)")
        continue

    if tbl in PRD_DELTA_TABLES:
        s3_path = PRD_DELTA_TABLES[tbl]
        try:
            df = spark.read.format("delta").load(s3_path)
            df.createOrReplaceTempView(tbl)
            print(f"    OK from Delta {s3_path} ({len(df.columns)} cols)")
            continue
        except Exception as e:
            print(f"    WARN: Delta read failed ({e}), trying Parquet")
            try:
                df = spark.read.parquet(s3_path)
                df.createOrReplaceTempView(tbl)
                print(f"    OK from Parquet {s3_path} ({len(df.columns)} cols)")
                continue
            except Exception as e2:
                print(f"    WARN: Parquet also failed ({e2})")

    try:
        df = glueContext.create_dynamic_frame.from_catalog(
            database=GLUE_DB, table_name=tbl
        ).toDF()
        df.createOrReplaceTempView(tbl)
        print(f"    OK ({len(df.columns)} cols)")
    except Exception as e:
        print(f"    WARN: {tbl} failed ({e})")


# ============================================================================
# Helper: run SQL, register as temp view, optionally write to S3/Glue
# ============================================================================
def materialize_view(view_name, sql, write_to_catalog=True):
    """Execute SQL, register as Spark temp view. Optionally write to S3 + Glue catalog."""
    print(f"\n--- Building: {view_name} ---")
    df = spark.sql(sql)
    df.createOrReplaceTempView(view_name)
    print(f"  Columns: {len(df.columns)}")

    if write_to_catalog:
        path = f"s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}/{view_name}/"
        print(f"  Writing to {path}")
        df.write.mode("overwrite").format("parquet").option("path", path).saveAsTable(
            f"{GLUE_DB}.{view_name}"
        )
        print(f"  Registered: {GLUE_DB}.{view_name}")

    return df


# ============================================================================
# Step 2: Build intermediate materialized views (Spark SQL)
# ============================================================================

# --- 2a. BookDisplayTypeIndex ---
# Redshift: dateadd(month, 7, x) -> Spark: add_months(x, 7)
materialize_view("bookdisplaytypeindex", """
SELECT
    a.BookName,
    a.BookType,
    a.FiscalGroupName,
    ROW_NUMBER() OVER (
        ORDER BY a.FiscalGroupName DESC, a.beginDateTime ASC
    ) AS BookIndex
FROM (
    SELECT
        br.Revision_Label AS BookName,
        fy.Fiscal_Year_Label AS FiscalGroupName,
        'BUDGET' AS BookType,
        br.Added_Datetime AS beginDateTime
    FROM plutus_budget_revision br
    INNER JOIN chronus_fiscal_year fy ON br.Fiscal_Year_Id = fy.Fiscal_Year_Id
    WHERE YEAR(fy.Begin_Date) >= YEAR(current_timestamp()) - 8
    UNION
    SELECT
        fy.Fiscal_Year_Label AS BookName,
        fy.Fiscal_Year_Label AS FiscalGroupName,
        'ACTUAL' AS BookType,
        add_months(fy.Begin_Date, 7) AS beginDateTime
    FROM chronus_fiscal_year fy
    WHERE YEAR(fy.Begin_Date) >= YEAR(current_timestamp()) - 8
) a
""")

# --- 2b. forecastEnrollment ---
materialize_view("forecastenrollment", """
SELECT
    pbr.Revision_Label AS Book,
    ttl.GL_Program_Cd,
    ttl.GP_DB_Name,
    ttl.GL_Term_Cd AS Term_Cd,
    fy.Fiscal_Year_Label,
    ttl.Nbr_Non_Discount_Students,
    ttl.Nbr_Discount_Students,
    concat(ttl.GP_DB_Name, ' ', ttl.GL_Program_Cd) AS `Program Groupings Key`
FROM (
    SELECT
        pbe.Budget_Revision_Id,
        pbe.GL_Program_Cd,
        pbe.GP_DB_Name,
        pbe.GL_Term_Cd,
        pbe.Nbr_Non_Discount_Students,
        pbe.Nbr_Discount_Students
    FROM plutus_budget_enrollment pbe
) ttl
INNER JOIN plutus_budget_revision pbr ON ttl.Budget_Revision_Id = pbr.Budget_Revision_Id
INNER JOIN chronus_fiscal_year fy ON pbr.fiscal_year_id = fy.fiscal_year_id
""")

# --- 2c. forecastEnrollmentTotals ---
materialize_view("forecastenrollmenttotals", """
SELECT
    pbr.Revision_Label,
    ttl.GL_Program_Cd,
    ttl.GP_DB_Name,
    fy.Fiscal_Year_Label,
    SUM(COALESCE(ttl.summerHeadCount, 0)) AS `Summer Head Count`,
    SUM(COALESCE(ttl.fallHeadCount, 0)) - SUM(COALESCE(ttl.AYHeadCount, 0)) AS `Fall Head Count`,
    SUM(COALESCE(ttl.AYHeadCount, 0)) AS `AY Head Count`,
    SUM(COALESCE(ttl.springHeadCount, 0)) AS `Spring Head Count`,
    SUM(
        COALESCE(ttl.summerHeadCount, 0) + COALESCE(ttl.fallHeadCount, 0) + COALESCE(ttl.springHeadCount, 0)
    ) AS `Total Head Count`,
    concat(ttl.GP_DB_Name, ' ', ttl.GL_Program_Cd) AS `Program Groupings Key`
FROM (
    SELECT
        pbe.Budget_Revision_Id,
        pbe.GL_Program_Cd,
        pbe.GP_DB_Name,
        CASE WHEN pbe.GL_Term_Cd = '01' THEN pbe.Nbr_Non_Discount_Students ELSE NULL END AS summerHeadCount,
        CASE WHEN pbe.GL_Term_Cd = '02' THEN pbe.Nbr_Non_Discount_Students ELSE NULL END AS fallHeadCount,
        CASE WHEN pbe.GL_Term_Cd = '03' THEN pbe.Nbr_Discount_Students ELSE NULL END AS AYHeadCount,
        CASE WHEN pbe.GL_Term_Cd = '03' THEN pbe.Nbr_Non_Discount_Students ELSE NULL END AS springHeadCount
    FROM plutus_budget_enrollment pbe
) ttl
INNER JOIN plutus_budget_revision pbr ON ttl.Budget_Revision_Id = pbr.Budget_Revision_Id
INNER JOIN chronus_fiscal_year fy ON pbr.Fiscal_Year_Id = fy.Fiscal_Year_Id
GROUP BY
    ttl.Budget_Revision_Id,
    pbr.Revision_Label,
    ttl.GL_Program_Cd,
    ttl.GP_DB_Name,
    fy.Fiscal_Year_Label
""")

# --- 2d. FX_Book_Rates ---
materialize_view("fx_book_rates", """
SELECT
    fx.CURRENCY_CODE,
    fx.Currency_Per_USD,
    br.Revision_Label,
    CAST(fy.Fiscal_Year_Id AS INT) AS Fiscal_Year_Id,
    fy.Fiscal_Year_Label,
    fy.Begin_Date,
    CASE WHEN defaultTable.Fiscal_Year_Id IS NULL THEN 0 ELSE 1 END AS Default_Book
FROM plutus_budget_revision_currency_fx fx
INNER JOIN plutus_budget_revision br ON br.Budget_Revision_Id = fx.Budget_Revision_Id
INNER JOIN chronus_fiscal_year fy ON fy.Fiscal_Year_Id = br.Fiscal_Year_Id
LEFT JOIN (
    SELECT
        pbr.Fiscal_Year_Id,
        MAX(pbr.Added_Datetime) AS Added_Datetime
    FROM plutus_budget_revision pbr
    INNER JOIN chronus_fiscal_year cfy ON pbr.Fiscal_Year_Id = cfy.Fiscal_Year_Id
    WHERE YEAR(cfy.Begin_Date) >= YEAR(current_timestamp()) - 8
    GROUP BY pbr.Fiscal_Year_Id
) defaultTable ON fy.Fiscal_Year_Id = defaultTable.Fiscal_Year_Id
    AND br.Added_Datetime = defaultTable.Added_Datetime
WHERE YEAR(fy.Begin_Date) >= YEAR(current_timestamp()) - 8
""")

# --- 2e. fx_rates_hist (pass-through, already in catalog) ---
# No transformation needed - temp view already loaded from raw table.

# --- 2f. historic_all_financial_transactions (pass-through) ---
# No transformation needed.

# --- 2g. Slicer_FXBookRates ---
# Redshift: CURRENT_TIMESTAMP + 720 -> Spark: current_timestamp() + INTERVAL 720 DAYS
materialize_view("slicer_fxbookrates", """
SELECT
    a.Book,
    ROW_NUMBER() OVER (ORDER BY a.Added_Datetime DESC) AS BookIndex
FROM (
    SELECT
        pbr.Revision_Label AS Book,
        pbr.Added_Datetime
    FROM plutus_budget_revision pbr
    INNER JOIN chronus_fiscal_year cfy ON pbr.Fiscal_Year_Id = cfy.Fiscal_Year_Id
    WHERE YEAR(cfy.Begin_Date) >= YEAR(current_timestamp()) - 8
    UNION
    SELECT
        'DEFAULT' AS Book,
        CAST(date_add(current_timestamp(), 720) AS TIMESTAMP) AS Added_Datetime
) a
""")

# --- 2h. Program_Groupings ---
materialize_view("program_groupings", """
SELECT
    catAndIncStmtQry.*,
    CASE
        WHEN catAndIncStmtQry.GL_Program_Cd = '5326029' THEN 'SAF'
        WHEN catAndIncStmtQry.GL_Program_Cd = '9700901' THEN 'IES'
        WHEN catAndIncStmtQry.glProgramCategory = '00' THEN 'Other'
        WHEN catAndIncStmtQry.glProgramCategory IN ('01','02','03','05','10','12','15','40') THEN 'IES'
        WHEN catAndIncStmtQry.glProgramCategory = '20' AND catAndIncStmtQry.incomeStatement = 'IES' THEN 'IES'
        WHEN catAndIncStmtQry.glProgramCategory = '20' AND catAndIncStmtQry.incomeStatement = 'International' THEN 'SAF'
        WHEN catAndIncStmtQry.glProgramCategory IN ('51','55','56','57','58','67','68','70','79','90','94','95','96','99') THEN 'SAF'
        WHEN catAndIncStmtQry.glProgramCategory IN ('09','30') THEN 'IP'
        WHEN catAndIncStmtQry.glProgramCategory IN ('60','61','89') THEN 'CP'
        ELSE 'Unknown'
    END AS businessLine,
    CASE
        WHEN catAndIncStmtQry.GL_Program_Cd = '5326029' THEN 'SAF CP'
        WHEN catAndIncStmtQry.GL_Program_Cd = '9700901' THEN 'Virtual World Discoveries'
        WHEN catAndIncStmtQry.glProgramCategory = '00' THEN 'Other'
        WHEN catAndIncStmtQry.glProgramCategory IN ('01','02','05','12','15','40') THEN 'Signature ("Standard")'
        WHEN catAndIncStmtQry.glProgramCategory = '08' THEN 'Flexible Study Abroad'
        WHEN catAndIncStmtQry.glProgramCategory = '09' THEN 'Virtual Internships'
        WHEN catAndIncStmtQry.glProgramCategory = '10' THEN 'Virtual Study Abroad'
        WHEN catAndIncStmtQry.glProgramCategory = '20' THEN 'Direct Enrollment'
        WHEN catAndIncStmtQry.glProgramCategory = '30' THEN 'Full-time Internships'
        WHEN catAndIncStmtQry.glProgramCategory IN ('51','57','70','76') THEN 'SAF at IES Abroad Signature'
        WHEN catAndIncStmtQry.glProgramCategory IN ('55','74') THEN 'SAF at IES Abroad DE'
        WHEN catAndIncStmtQry.glProgramCategory = '56' THEN 'SAF at IES Internships'
        WHEN catAndIncStmtQry.glProgramCategory IN ('58','81','83','90') THEN 'SAF Island at IES Center'
        WHEN catAndIncStmtQry.glProgramCategory IN ('60','61','89') THEN 'CP at IES Abroad Center'
        WHEN catAndIncStmtQry.glProgramCategory IN ('67','68','77','78','79','80') THEN 'SAF CP at IES Center'
        WHEN catAndIncStmtQry.glProgramCategory IN ('94','95','99') THEN 'SAF CP'
        WHEN catAndIncStmtQry.glProgramCategory = '96' THEN 'SAF Island at SAF Host'
        ELSE 'Unknown'
    END AS programModel
FROM (
    SELECT
        GL_Program_Cd,
        GL_Program_Name,
        GP_DB_Name,
        SUBSTRING(GL_Program_Cd, 4, 2) AS glProgramCategory,
        CASE
            WHEN SUBSTRING(GL_Program_Cd, 4, 2) IN ('51','55','56','57','58','67','68','90','94','95','96','99')
                OR GP_DB_Name IN ('BEI2','CHN2','GUA2','JPN2','KOR2','SHA2','ISI2','SAF1','SAF2') THEN 'International'
            WHEN GP_DB_Name IN ('ZPR2','LRHC') THEN 'Other'
            ELSE 'IES'
        END AS incomeStatement
    FROM chronus_gl_program
) catAndIncStmtQry
""")

# --- 2i. MiniStudentDetailsAndStatusHistory ---
# Redshift: INTERVAL '15 DAY' + date -> Spark: date_add(date, 15)
# Correlated subqueries should work in Spark SQL
materialize_view("ministudentdetailsandstatushistory", """
SELECT
    concat(q.GP_DB_Name, ' ', q.GL_Program_Cd) AS `Program Groupings Key`,
    q.Fiscal_Year,
    q.Fiscal_Year AS Book,
    q.GL_Program_Cd,
    q.GP_DB_Name,
    q.`Program Name`,
    q.`Application Status`,
    q.`Enrollment Type`,
    q.Term_Category,
    CASE
        WHEN q.Term_Category = 'Summer' THEN '01'
        WHEN q.Term_Category = 'Fall' THEN '02'
        ELSE '03'
    END AS Term_Cd,
    q.Term_Length,
    CAST(q.Student_Term_Admit_Id AS INT) AS Student_Term_Admit_Id,
    q.Student_Status_Type
FROM (
    /* IESApplicationDB Records */
    SELECT
        CASE
            WHEN t.Term_Category = 'Fall' OR t.Term_Category = 'Spring' THEN fy.Fiscal_Year_Label
            WHEN cpt.End_Dt < fy.Begin_Date THEN (
                SELECT MAX(Fiscal_Year_Label) FROM chronus_fiscal_year
                WHERE Fiscal_Year_Id = t.Fiscal_Year_Id - 1
            )
            ELSE fy.Fiscal_Year_Label
        END AS Fiscal_Year,
        SUBSTRING(cpt.CMC_Program_Version_Cd, 1, 7) AS GL_Program_Cd,
        gpdb.GP_DB_Name,
        cpt.Program_Local_Label AS `Program Name`,
        t.Term_Category,
        sta.Term_Length,
        sta.Student_Status_Type,
        CASE
            WHEN sta.Student_Status_Type IN (
                'Provisional Acceptance','Applicant with Complete File','Applicant with Incomplete File',
                'Admitted Applicant','Early Acceptance','Transfer From Other Center',
                'Transfer From Other Program','Confirmed Student',
                'Second Enroll Different Program','Second Enroll Same Program',
                'Wait listed--app complete') THEN 'Applicant'
            WHEN sta.Student_Status_Type IN (
                'Arrived Student','Quit After Program Start Date','Alumni') THEN 'Enrolled'
            WHEN sta.Student_Status_Type IN (
                'Withdrew Before Admit Decision','Denied Admission',
                'Withdrew After Admit Decision','Student Deferred Admission') THEN 'Withdrawn'
            ELSE 'N/A'
        END AS `Application Status`,
        CASE
            WHEN sta.Term_Length IN ('Academic Year - Term Two', 'Split Year – Term Two')
                THEN 'Continuing Enrollment'
            ELSE 'New Enrollment'
        END AS `Enrollment Type`,
        sta.Student_Term_Admit_Id
    FROM chronus_student_term_admit sta
    INNER JOIN chronus_person p ON sta.Person_Id = p.Person_Id
    INNER JOIN chronus_center_program_term cpt
        ON sta.Center_Id = cpt.Center_Id
        AND sta.Program_Id = cpt.Program_Id
        AND sta.Term_Id = cpt.Term_Id
    INNER JOIN chronus_term t ON cpt.Term_Id = t.Term_Id
    INNER JOIN chronus_fiscal_year fy ON t.Fiscal_Year_Id = fy.Fiscal_Year_Id
    INNER JOIN chronus_great_plains_db gpdb ON cpt.Center_Id = gpdb.Center_Id
    WHERE p.Person_Type = 'Student' AND t.Term_Id >= 475

    UNION

    /* SAFApplicationDB Records */
    SELECT
        CASE
            WHEN t.Term_Category = 'Summer' OR t.Term_Category = 'Fall' THEN fy.Fiscal_Year_Label
            WHEN cpt.End_Dt > date_add(fy.End_Date, 15) THEN (
                SELECT MAX(Fiscal_Year_Label) FROM saf_chronus_fiscal_year
                WHERE Fiscal_Year_Id = t.Fiscal_Year_Id + 1
            )
            ELSE fy.Fiscal_Year_Label
        END AS Fiscal_Year,
        SUBSTRING(cpt.CMC_Program_Version_Cd, 1, 7) AS GL_Program_Cd,
        CASE
            WHEN glp.GP_DB_Name = 'SAF2' THEN glp.GP_DB_Name
            ELSE (
                SELECT MAX(Center_Abbr) FROM chronus_center
                WHERE SUBSTRING(cpt.CMC_Program_Version_Cd, 1, 3) = CMC_Campus_Cd
            )
        END AS GP_DB_Name,
        cpt.Program_Local_Label AS `Program Name`,
        t.Term_Category,
        sta.Term_Length,
        sta.Student_Status_Type,
        CASE
            WHEN sta.Student_Status_Type IN (
                'Provisional Acceptance','Applicant with Incomplete File','Applicant with Complete File',
                'Application Pending','Application Received','Application Submitted',
                'Admitted Applicant','Complete','Conditional Admission','Early Acceptance',
                'Transfer From Other Center','Transfer From Other Program',
                'Confirmed Student','Waitlisted') THEN 'Applicant'
            WHEN sta.Student_Status_Type IN (
                'Active at IES Center','Alumni of IES Center','Arrived Student',
                'Withdrawal','Alumni') THEN 'Enrolled'
            WHEN sta.Student_Status_Type IN (
                'Withdrew Before Admit Decision','Application Rejected - Not Used',
                'Denied Admission','Application Rejected','Withdrew After Admit Decision',
                'Deferred','Defer Admission','Defer Application',
                'Undecided Program Change') THEN 'Withdrawn'
            ELSE 'N/A'
        END AS `Application Status`,
        CASE
            WHEN sta.Term_Length IN (
                'EXTCONT','EXTEND','EXTPROG','FACO','FAEND','FAPR',
                'SPCO','SPEND','SPPR','SPQTRCO','SPQTREND',
                'SUCO','SUEND','SUPR','WQTCO','WQPROG','WQEND')
                THEN 'Continuing Enrollment'
            ELSE 'New Enrollment'
        END AS `Enrollment Type`,
        sta.Student_Term_Admit_Id
    FROM saf_chronus_student_term_admit sta
    INNER JOIN saf_chronus_person p ON sta.Person_Id = p.Person_Id
    INNER JOIN saf_chronus_center_program_term cpt
        ON sta.Center_Id = cpt.Center_Id
        AND sta.Program_Id = cpt.Program_Id
        AND sta.Term_Id = cpt.Term_Id
    INNER JOIN saf_chronus_term t ON cpt.Term_Id = t.Term_Id
    INNER JOIN saf_chronus_fiscal_year fy ON t.Fiscal_Year_Id = fy.Fiscal_Year_Id
    LEFT OUTER JOIN chronus_gl_program glp
        ON SUBSTRING(cpt.CMC_Program_Version_Cd, 1, 7) = glp.GL_Program_Cd
        AND 'SAF2' = glp.GP_DB_Name
    WHERE p.Person_Type = 'Student'
) q
""")

# --- 2j. CurrentGL_Budget_Transactions (bonus — not in qsreport join but useful) ---
materialize_view("currentgl_budget_transactions", """
SELECT
    gpd.GP_DB_Name,
    td.Period_Id AS Period,
    td.Functional_Currency AS fncCur,
    td.Transaction_Date AS Transaction_Datetime,
    td.Transaction_Long_Desc AS Description_Long,
    NULLIF(td.Vendor_Name, '') AS `Vendor Name`,
    fy.Fiscal_Year_Label AS Book,
    gpd.Label AS Center,
    gpd.CURRENCY_CODE AS CtrCur,
    (td.Credit_Amount - td.Debit_Amount) AS AmountInFncCur,
    CASE WHEN CAST(td.Natural_Account_Id AS INT) < 500000 THEN 'Revenue' ELSE 'Expense' END AS `Account Type`,
    ac.Category_Name AS `Acct Cat Per Accounting`,
    td.Budget_Dept_Nbr AS DeptCd,
    bd.Budget_Dept_Name AS DeptLabel,
    td.Natural_Account_Id AS AccountCd,
    na.Natural_Account_Name AS AccountLabel,
    td.GL_Term_Cd AS TermCd,
    t.GL_Term_Name AS TermLabel,
    td.GL_Program_Cd AS ProgramCd,
    p.GL_Program_Name AS ProgramLabel,
    td.Project_Id AS ProjectCd,
    prj.Project_Name AS ProjectLabel,
    CAST(fy.Fiscal_Year_Id AS INT) AS Fiscal_Year_Id
FROM chronus_gl_transaction_detail td
INNER JOIN chronus_great_plains_db gpd ON gpd.GP_DB_Name = td.GP_DB_Name
INNER JOIN chronus_gl_budget_dept bd ON bd.Budget_Dept_Nbr = td.Budget_Dept_Nbr AND bd.GP_DB_Name = td.GP_DB_Name
INNER JOIN chronus_gl_natural_account na ON na.Natural_Account_Id = td.Natural_Account_Id AND na.GP_DB_Name = td.GP_DB_Name
INNER JOIN chronus_gl_program p ON p.GL_Program_Cd = td.GL_Program_Cd AND p.GP_DB_Name = td.GP_DB_Name
INNER JOIN chronus_gl_project prj ON prj.Project_Id = td.Project_Id AND prj.GP_DB_Name = td.GP_DB_Name
INNER JOIN chronus_gl_term t ON t.GL_Term_Cd = td.GL_Term_Cd AND t.GP_DB_Name = td.GP_DB_Name
INNER JOIN chronus_fiscal_year fy ON fy.Fiscal_Year_Id = td.Fiscal_Year_Id
LEFT OUTER JOIN plutus_gl_account_category ac ON ac.Account_Prefix = SUBSTRING(CAST(td.Natural_Account_Id AS STRING), 1, 2)
WHERE td.GL_Closed_Ind = 0
    AND YEAR(fy.Begin_Date) >= YEAR(current_timestamp()) - 8

UNION ALL

SELECT
    gpd.GP_DB_Name,
    ber.Period_Id AS Period,
    be.CURRENCY_CODE AS fncCur,
    NULL AS Transaction_Datetime,
    be.Entry_Description AS Description_Long,
    NULL AS `Vendor Name`,
    br.Revision_Label AS Book,
    gpd.Label AS Center,
    gpd.CURRENCY_CODE AS CtrCur,
    CASE WHEN CAST(be.Natural_Account_Id AS INT) < 500000 THEN ber.Period_Amount ELSE ber.Period_Amount * -1 END AS AmountInFncCur,
    CASE WHEN CAST(be.Natural_Account_Id AS INT) < 500000 THEN 'Revenue' ELSE 'Expense' END AS `Account Type`,
    ac.Category_Name AS `Acct Cat Per Accounting`,
    be.Budget_Dept_Nbr AS DeptCd,
    bd.Budget_Dept_Name AS DeptLabel,
    be.Natural_Account_Id AS AccountCd,
    na.Natural_Account_Name AS AccountLabel,
    be.GL_Term_Cd AS TermCd,
    t.GL_Term_Name AS TermLabel,
    be.GL_Program_Cd AS ProgramCd,
    p.GL_Program_Name AS ProgramLabel,
    be.Project_Id AS ProjectCd,
    prj.Project_Name AS ProjectLabel,
    CAST(fy.Fiscal_Year_Id AS INT) AS Fiscal_Year_Id
FROM plutus_budget_entry be
LEFT OUTER JOIN chronus_great_plains_db gpd ON gpd.GP_DB_Name = be.GP_DB_Name
INNER JOIN plutus_budget_revision br ON br.Budget_Revision_Id = be.Budget_Revision_Id
INNER JOIN chronus_fiscal_year fy ON fy.Fiscal_Year_Id = br.Fiscal_Year_Id
INNER JOIN chronus_gl_budget_dept bd ON bd.Budget_Dept_Nbr = be.Budget_Dept_Nbr AND bd.GP_DB_Name = be.GP_DB_Name
INNER JOIN chronus_gl_natural_account na ON na.Natural_Account_Id = be.Natural_Account_Id AND na.GP_DB_Name = be.GP_DB_Name
INNER JOIN chronus_gl_program p ON p.GL_Program_Cd = be.GL_Program_Cd AND p.GP_DB_Name = be.GP_DB_Name
INNER JOIN chronus_gl_project prj ON prj.Project_Id = be.Project_Id AND prj.GP_DB_Name = be.GP_DB_Name
INNER JOIN chronus_gl_term t ON t.GL_Term_Cd = be.GL_Term_Cd AND t.GP_DB_Name = be.GP_DB_Name
INNER JOIN plutus_budget_entry_proration ber ON ber.Entry_Id = be.Entry_Id
LEFT OUTER JOIN plutus_gl_account_category ac ON ac.Account_Prefix = SUBSTRING(CAST(be.Natural_Account_Id AS STRING), 1, 2)
INNER JOIN plutus_department_budget db ON db.Budget_Revision_Id = be.Budget_Revision_Id
    AND db.GP_DB_Name = be.GP_DB_Name AND db.Org_Dept_Code = be.Org_Dept_Code
WHERE db.Budget_Status_Type <> 'Locked'
    AND YEAR(fy.Begin_Date) >= YEAR(current_timestamp()) - 8
""")


# ============================================================================
# Step 3: SKIP final big join (historic_financial_qsreport)
#
# The final join is h LEFT JOIN fe ON (book,gp_db_name) LEFT JOIN msd ON
# (book,gp_db_name) — both are many-to-many and explode 6.6M rows into
# billions.  All 9 intermediate views are now materialized in the Glue
# catalog.  QuickSight can join them at query time with user-selected
# filters (fiscal year, center) which keeps result sets manageable.
#
# Tables available for QuickSight:
#   historic_all_financial_transactions  (6.6M)  — base transactions
#   currentgl_budget_transactions        (290K)  — current GL + budget
#   bookdisplaytypeindex                 (37)    — book lookup
#   forecastenrollment                   (797K)  — enrollment by term
#   forecastenrollmenttotals             (18K)   — enrollment aggregates
#   fx_book_rates                        (0)     — FX per book (empty)
#   fx_rates_hist                        (7.7K)  — historical FX rates
#   slicer_fxbookrates                   (28)    — FX book slicer
#   program_groupings                    (12K)   — program classification
#   ministudentdetailsandstatushistory   (195K)  — student details
# ============================================================================

# ============================================================================
# Step 3b: Partial join — safe 1:1 / 1:few lookups only
# Excludes: forecastenrollment (many-to-many on book,gp_db_name)
#           ministudentdetailsandstatushistory (many-to-many on book,gp_db_name)
#           forecastenrollmenttotals, fx_book_rates (chain off fe)
# Includes: bookdisplaytypeindex, program_groupings, fx_rates_hist,
#           slicer_fxbookrates — all joined directly on h
# ============================================================================
materialize_view("historic_financial_qsreport_partial", sql="""
SELECT
    h.period,
    h.fnccur,
    h.transaction_datetime,
    h.description_long,
    h.vendor_name,
    h.book,
    h.center,
    h.ctrcur,
    h.amount_in_fnc_cur,
    h.account_type,
    h.acct_cat_per_accounting,
    h.deptcd,
    h.deptlabel,
    h.accountcd,
    h.accountlabel,
    h.termcd,
    h.termlabel,
    h.programcd,
    h.programlabel,
    h.projectcd,
    h.projectlabel,
    h.fiscal_year_id,
    h.gp_db_name,
    b.BookName,
    b.BookType,
    b.FiscalGroupName,
    pg.GL_Program_Name,
    pg.GP_DB_Name AS pg_gp_db_name,
    pg.glProgramCategory,
    pg.incomeStatement,
    pg.businessLine,
    pg.programModel,
    fxh.ratetype,
    fxh.curncyid,
    fxh.xchgrate,
    fxh.effxchgrate,
    fxh.exchdate,
    fxh.year1,
    sfx.Book AS sfx_book,
    sfx.BookIndex
FROM historic_all_financial_transactions h
LEFT JOIN bookdisplaytypeindex b
    ON h.book = b.BookName
LEFT JOIN program_groupings pg
    ON h.programcd = pg.GL_Program_Cd
    AND h.gp_db_name = pg.GP_DB_Name
LEFT JOIN fx_rates_hist fxh
    ON h.fnccur = fxh.curncyid
    AND h.period = fxh.periodid
LEFT JOIN slicer_fxbookrates sfx
    ON h.book = sfx.Book
""")

print("\n=== All views materialized successfully ===")
job.commit()
