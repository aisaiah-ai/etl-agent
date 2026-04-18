-- seed_redshift.sql — Seeds local PostgreSQL (Redshift stand-in) with sample schemas, tables, and views.
-- PostgreSQL is wire-compatible with Redshift for most analytical SQL.

-- ============================================================================
-- Source tables (simulating what lives in S3 / Glue catalog external tables)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS spectrum_schema;

-- Patients table (would be an external table from Glue catalog in real Redshift)
CREATE TABLE IF NOT EXISTS public.patients (
    patient_id       VARCHAR(50) PRIMARY KEY,
    first_name       VARCHAR(100),
    last_name        VARCHAR(100),
    date_of_birth    DATE,
    gender           VARCHAR(10),
    zip_code         VARCHAR(10),
    state            VARCHAR(2),
    phone            VARCHAR(20),
    email            VARCHAR(200),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Claims table
CREATE TABLE IF NOT EXISTS public.claims (
    claim_id         VARCHAR(50) PRIMARY KEY,
    patient_id       VARCHAR(50) REFERENCES public.patients(patient_id),
    provider_id      VARCHAR(50),
    claim_type       VARCHAR(20),
    service_date     DATE,
    diagnosis_code   VARCHAR(20),
    procedure_code   VARCHAR(20),
    charged_amount   NUMERIC(12,2),
    paid_amount      NUMERIC(12,2),
    status           VARCHAR(20),
    submitted_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Encounters table
CREATE TABLE IF NOT EXISTS public.encounters (
    encounter_id     VARCHAR(50) PRIMARY KEY,
    patient_id       VARCHAR(50) REFERENCES public.patients(patient_id),
    provider_id      VARCHAR(50),
    encounter_type   VARCHAR(50),
    encounter_date   DATE,
    discharge_date   DATE,
    facility_id      VARCHAR(50),
    primary_dx       VARCHAR(20),
    drg_code         VARCHAR(10),
    los_days         INTEGER
);

-- Payer plans (simulating Spectrum external table from Glue catalog)
CREATE TABLE IF NOT EXISTS spectrum_schema.payer_plans (
    plan_id          VARCHAR(50) PRIMARY KEY,
    plan_name        VARCHAR(200),
    payer_name       VARCHAR(200),
    plan_type        VARCHAR(50),
    effective_date   DATE,
    termination_date DATE
);

-- Providers
CREATE TABLE IF NOT EXISTS public.providers (
    provider_id      VARCHAR(50) PRIMARY KEY,
    provider_name    VARCHAR(200),
    specialty        VARCHAR(100),
    npi              VARCHAR(20),
    facility_id      VARCHAR(50),
    state            VARCHAR(2)
);

-- ============================================================================
-- Sample data
-- ============================================================================

INSERT INTO public.patients VALUES
    ('P001', 'John', 'Doe', '1985-03-15', 'M', '10001', 'NY', '212-555-0101', 'john.doe@example.com', '2024-01-01'),
    ('P002', 'Jane', 'Smith', '1990-07-22', 'F', '90210', 'CA', '310-555-0102', 'jane.smith@example.com', '2024-01-02'),
    ('P003', 'Robert', 'Johnson', '1975-11-30', 'M', '60601', 'IL', '312-555-0103', 'r.johnson@example.com', '2024-01-03'),
    ('P004', 'Maria', 'Garcia', '1988-06-14', 'F', '77001', 'TX', '713-555-0104', 'maria.g@example.com', '2024-02-01'),
    ('P005', 'David', 'Wilson', '1965-01-20', 'M', '30301', 'GA', '404-555-0105', 'dwilson@example.com', '2024-02-15')
ON CONFLICT DO NOTHING;

INSERT INTO public.claims VALUES
    ('C001', 'P001', 'DR001', 'professional', '2024-06-01', 'J06.9', '99213', 250.00, 175.00, 'paid', '2024-06-02'),
    ('C002', 'P001', 'DR002', 'institutional', '2024-07-15', 'I10', '99214', 500.00, 350.00, 'paid', '2024-07-16'),
    ('C003', 'P002', 'DR001', 'professional', '2024-06-10', 'M54.5', '99203', 300.00, 210.00, 'paid', '2024-06-11'),
    ('C004', 'P003', 'DR003', 'professional', '2024-08-01', 'E11.9', '99215', 450.00, 315.00, 'pending', '2024-08-02'),
    ('C005', 'P004', 'DR001', 'professional', '2024-09-01', 'J06.9', '99213', 250.00, 0.00, 'denied', '2024-09-02'),
    ('C006', 'P002', 'DR002', 'institutional', '2024-09-15', 'I10', '99214', 500.00, 350.00, 'paid', '2024-09-16'),
    ('C007', 'P005', 'DR003', 'professional', '2024-10-01', 'E78.5', '99214', 400.00, 280.00, 'paid', '2024-10-02')
ON CONFLICT DO NOTHING;

INSERT INTO public.encounters VALUES
    ('E001', 'P001', 'DR001', 'outpatient', '2024-06-01', '2024-06-01', 'F001', 'J06.9', NULL, 0),
    ('E002', 'P001', 'DR002', 'inpatient', '2024-07-15', '2024-07-18', 'F002', 'I10', '291', 3),
    ('E003', 'P002', 'DR001', 'outpatient', '2024-06-10', '2024-06-10', 'F001', 'M54.5', NULL, 0),
    ('E004', 'P003', 'DR003', 'emergency', '2024-08-01', '2024-08-01', 'F003', 'E11.9', NULL, 0),
    ('E005', 'P005', 'DR003', 'outpatient', '2024-10-01', '2024-10-01', 'F003', 'E78.5', NULL, 0)
ON CONFLICT DO NOTHING;

INSERT INTO spectrum_schema.payer_plans VALUES
    ('PLN001', 'Blue Cross PPO', 'Blue Cross Blue Shield', 'PPO', '2024-01-01', '2025-12-31'),
    ('PLN002', 'Aetna HMO', 'Aetna', 'HMO', '2024-01-01', '2025-12-31'),
    ('PLN003', 'UHC Choice Plus', 'UnitedHealthcare', 'POS', '2024-01-01', '2025-12-31')
ON CONFLICT DO NOTHING;

INSERT INTO public.providers VALUES
    ('DR001', 'Dr. Sarah Chen', 'Family Medicine', '1234567890', 'F001', 'NY'),
    ('DR002', 'Dr. Michael Park', 'Cardiology', '2345678901', 'F002', 'NY'),
    ('DR003', 'Dr. Lisa Martinez', 'Internal Medicine', '3456789012', 'F003', 'IL')
ON CONFLICT DO NOTHING;

-- ============================================================================
-- Views (the Redshift views we want to migrate to Glue PySpark jobs)
-- ============================================================================

-- View 1: Patient demographics summary
CREATE OR REPLACE VIEW public.vw_patient_demographics AS
SELECT
    p.patient_id,
    p.first_name,
    p.last_name,
    p.date_of_birth,
    p.gender,
    p.zip_code,
    p.state,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, p.date_of_birth)) AS age
FROM public.patients p;

-- View 2: Claims with patient info (JOIN)
CREATE OR REPLACE VIEW public.vw_claims_with_patient AS
SELECT
    c.claim_id,
    c.patient_id,
    p.first_name,
    p.last_name,
    c.claim_type,
    c.service_date,
    c.diagnosis_code,
    c.procedure_code,
    c.charged_amount,
    c.paid_amount,
    c.status
FROM public.claims c
INNER JOIN public.patients p ON c.patient_id = p.patient_id;

-- View 3: Claims summary by patient (aggregation)
CREATE OR REPLACE VIEW public.vw_patient_claims_summary AS
SELECT
    c.patient_id,
    p.first_name,
    p.last_name,
    COUNT(*) AS total_claims,
    SUM(c.charged_amount) AS total_charged,
    SUM(c.paid_amount) AS total_paid,
    AVG(c.paid_amount) AS avg_paid,
    MIN(c.service_date) AS first_service_date,
    MAX(c.service_date) AS last_service_date
FROM public.claims c
INNER JOIN public.patients p ON c.patient_id = p.patient_id
GROUP BY c.patient_id, p.first_name, p.last_name;

-- View 4: Encounters with claims (multi-table join)
CREATE OR REPLACE VIEW public.vw_encounter_claims AS
SELECT
    e.encounter_id,
    e.patient_id,
    p.first_name,
    p.last_name,
    e.encounter_type,
    e.encounter_date,
    e.discharge_date,
    e.los_days,
    c.claim_id,
    c.charged_amount,
    c.paid_amount,
    c.status AS claim_status
FROM public.encounters e
INNER JOIN public.patients p ON e.patient_id = p.patient_id
LEFT JOIN public.claims c ON e.patient_id = c.patient_id
    AND e.encounter_date = c.service_date;

-- View 5: Provider performance (aggregation with multiple metrics)
CREATE OR REPLACE VIEW public.vw_provider_performance AS
SELECT
    pr.provider_id,
    pr.provider_name,
    pr.specialty,
    COUNT(DISTINCT c.patient_id) AS unique_patients,
    COUNT(c.claim_id) AS total_claims,
    SUM(c.charged_amount) AS total_charged,
    SUM(c.paid_amount) AS total_paid,
    CASE
        WHEN SUM(c.charged_amount) > 0
        THEN ROUND(SUM(c.paid_amount) / SUM(c.charged_amount) * 100, 2)
        ELSE 0
    END AS payment_rate_pct
FROM public.providers pr
LEFT JOIN public.claims c ON pr.provider_id = c.provider_id
GROUP BY pr.provider_id, pr.provider_name, pr.specialty;
