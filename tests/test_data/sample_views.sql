-- sample_views.sql
-- Sample Redshift view definitions for testing the ETL migration pipeline.

-- ============================================================================
-- View 1: Simple SELECT — patient demographics
-- ============================================================================
CREATE OR REPLACE VIEW public.vw_patient_demographics AS
SELECT
    p.patient_id,
    p.first_name,
    p.last_name,
    p.date_of_birth,
    p.gender,
    p.zip_code,
    DATEDIFF(year, p.date_of_birth, GETDATE()) AS age,
    NVL(p.primary_language, 'English')          AS primary_language
FROM
    public.patients p
WHERE
    p.is_active = 1;


-- ============================================================================
-- View 2: JOINs with external (Spectrum) tables via Glue Data Catalog
-- ============================================================================
CREATE OR REPLACE VIEW public.vw_claims_enriched AS
SELECT
    c.claim_id,
    c.patient_id,
    c.service_date,
    c.procedure_code,
    c.diagnosis_code,
    c.billed_amount,
    c.paid_amount,
    c.billed_amount - c.paid_amount             AS patient_responsibility,
    p.first_name || ' ' || p.last_name          AS patient_name,
    pr.provider_name,
    pr.specialty,
    ext.payer_name,
    ext.plan_type
FROM
    public.claims c
    INNER JOIN public.patients p
        ON c.patient_id = p.patient_id
    INNER JOIN public.providers pr
        ON c.provider_id = pr.provider_id
    LEFT JOIN spectrum_schema.payer_plans ext
        ON c.payer_plan_id = ext.plan_id
WHERE
    c.service_date >= DATEADD(month, -12, GETDATE());


-- ============================================================================
-- View 3: Aggregations with Redshift-specific functions
-- ============================================================================
CREATE OR REPLACE VIEW public.vw_monthly_claim_summary AS
SELECT
    DATE_TRUNC('month', c.service_date)         AS claim_month,
    c.diagnosis_code,
    COUNT(*)                                    AS claim_count,
    COUNT(DISTINCT c.patient_id)                AS unique_patients,
    SUM(c.billed_amount)                        AS total_billed,
    SUM(c.paid_amount)                          AS total_paid,
    MEDIAN(c.paid_amount)                       AS median_paid,
    LISTAGG(DISTINCT c.procedure_code, ', ')
        WITHIN GROUP (ORDER BY c.procedure_code) AS procedure_codes,
    GETDATE()                                   AS report_generated_at
FROM
    public.claims c
GROUP BY
    DATE_TRUNC('month', c.service_date),
    c.diagnosis_code
HAVING
    COUNT(*) >= 5;


-- ============================================================================
-- View 4: Dependency chain — references vw_claims_enriched (View 2)
-- ============================================================================
CREATE OR REPLACE VIEW public.vw_high_cost_claims AS
SELECT
    ce.claim_id,
    ce.patient_name,
    ce.provider_name,
    ce.specialty,
    ce.payer_name,
    ce.billed_amount,
    ce.paid_amount,
    ce.patient_responsibility,
    ce.service_date,
    DECODE(
        ce.specialty,
        'Cardiology',   'Heart & Vascular',
        'Oncology',     'Cancer Care',
        'Orthopedics',  'Musculoskeletal',
        'General'
    ) AS service_line,
    CONVERT_TIMEZONE('UTC', 'US/Eastern', GETDATE()) AS report_tz
FROM
    public.vw_claims_enriched ce
WHERE
    ce.billed_amount > 10000
ORDER BY
    ce.billed_amount DESC;
