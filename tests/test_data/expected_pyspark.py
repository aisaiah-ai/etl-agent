"""Expected PySpark output for vw_patient_demographics (the simple SELECT view).

Used by test_translator.py to verify translation accuracy.
"""

EXPECTED_VW_PATIENT_DEMOGRAPHICS = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, current_timestamp, datediff, year

spark = SparkSession.builder.getOrCreate()

# Read source table
patients_df = spark.table("etl_agent_db.patients")

# Apply transformations
vw_patient_demographics_df = (
    patients_df.alias("p")
    .filter(col("p.is_active") == 1)
    .select(
        col("p.patient_id"),
        col("p.first_name"),
        col("p.last_name"),
        col("p.date_of_birth"),
        col("p.gender"),
        col("p.zip_code"),
        (
            datediff(current_timestamp(), col("p.date_of_birth")) / 365
        ).cast("int").alias("age"),
        coalesce(col("p.primary_language"), lit("English")).alias("primary_language"),
    )
)

vw_patient_demographics_df.createOrReplaceTempView("vw_patient_demographics")
'''.strip()
