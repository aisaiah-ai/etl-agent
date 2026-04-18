-- Seed script for dev Redshift Serverless
-- Exact replica of prod iesabroad_dw.financial_hist schema
-- Source: historic_financial_qsreport materialized view and its 9 source tables
--
-- Run via Redshift Query Editor v2 against the dev workgroup.

CREATE SCHEMA IF NOT EXISTS financial_hist;

-- 1. bookdisplaytypeindex
CREATE TABLE IF NOT EXISTS financial_hist.bookdisplaytypeindex (
    bookname        VARCHAR(16383),
    booktype        VARCHAR(6),
    fiscalgroupname VARCHAR(16383),
    bookindex       BIGINT
);

-- 2. forecastenrollment
CREATE TABLE IF NOT EXISTS financial_hist.forecastenrollment (
    book                       VARCHAR(16383),
    gl_program_cd              VARCHAR(16383),
    gp_db_name                 VARCHAR(16383),
    term_cd                    VARCHAR(16383),
    fiscal_year_label          VARCHAR(16383),
    nbr_non_discount_students  INTEGER,
    nbr_discount_students      INTEGER,
    "program groupings key"    VARCHAR(32767)
);

-- 3. forecastenrollmenttotals
CREATE TABLE IF NOT EXISTS financial_hist.forecastenrollmenttotals (
    revision_label           VARCHAR(16383),
    gl_program_cd            VARCHAR(16383),
    gp_db_name               VARCHAR(16383),
    fiscal_year_label        VARCHAR(16383),
    "summer head count"      BIGINT,
    "fall head count"        BIGINT,
    "ay head count"          BIGINT,
    "spring head count"      BIGINT,
    "total head count"       BIGINT,
    "program groupings key"  TEXT
);

-- 4. fx_book_rates
CREATE TABLE IF NOT EXISTS financial_hist.fx_book_rates (
    currency_code    VARCHAR(16383),
    currency_per_usd NUMERIC(18,12),
    revision_label   VARCHAR(16383),
    fiscal_year_id   INTEGER,
    fiscal_year_label VARCHAR(16383),
    begin_date       TIMESTAMP,
    default_book     INTEGER
);

-- 5. fx_rates_hist
CREATE TABLE IF NOT EXISTS financial_hist.fx_rates_hist (
    ratetype    VARCHAR(16383),
    nacntr      VARCHAR(16383),
    curncyid    VARCHAR(16383),
    rtclcmtd    VARCHAR(16383),
    xchgrate    DOUBLE PRECISION,
    effxchgrate DOUBLE PRECISION,
    exchdate    VARCHAR(16383),
    expndate    VARCHAR(16383),
    periodid    INTEGER,
    year1       INTEGER
);

-- 6. historic_all_financial_transactions (main fact table)
CREATE TABLE IF NOT EXISTS financial_hist.historic_all_financial_transactions (
    gp_db_name             VARCHAR(16383),
    period                 INTEGER,
    fnccur                 VARCHAR(16383),
    transaction_datetime   TIMESTAMP,
    description_long       VARCHAR(16383),
    vendor_name            VARCHAR(16383),
    book                   VARCHAR(16383),
    center                 VARCHAR(16383),
    ctrcur                 VARCHAR(16383),
    amount_in_fnc_cur      NUMERIC(23,5),
    account_type           VARCHAR(16383),
    acct_cat_per_accounting VARCHAR(16383),
    deptcd                 VARCHAR(16383),
    deptlabel              VARCHAR(16383),
    accountcd              VARCHAR(16383),
    accountlabel           VARCHAR(16383),
    termcd                 VARCHAR(16383),
    termlabel              VARCHAR(16383),
    programcd              VARCHAR(16383),
    programlabel           VARCHAR(16383),
    projectcd              VARCHAR(16383),
    projectlabel           VARCHAR(16383),
    fiscal_year_id         INTEGER
);

-- 7. ministudentdetailsandstatushistory
CREATE TABLE IF NOT EXISTS financial_hist.ministudentdetailsandstatushistory (
    "program groupings key" VARCHAR(16412),
    fiscal_year             VARCHAR(16383),
    book                    VARCHAR(16383),
    gl_program_cd           VARCHAR(28),
    gp_db_name              VARCHAR(16383),
    "program name"          VARCHAR(16383),
    "application status"    VARCHAR(9),
    "enrollment type"       VARCHAR(21),
    term_category           VARCHAR(16383),
    term_cd                 VARCHAR(2),
    term_length             VARCHAR(16383),
    student_term_admit_id   INTEGER,
    student_status_type     VARCHAR(16383)
);

-- 8. program_groupings
CREATE TABLE IF NOT EXISTS financial_hist.program_groupings (
    gl_program_cd     VARCHAR(16383),
    gl_program_name   VARCHAR(16383),
    gp_db_name        VARCHAR(16383),
    glprogramcategory VARCHAR(8),
    incomestatement   VARCHAR(13),
    businessline      VARCHAR(7),
    programmodel      VARCHAR(27)
);

-- 9. slicer_fxbookrates
CREATE TABLE IF NOT EXISTS financial_hist.slicer_fxbookrates (
    book      VARCHAR(16383),
    bookindex BIGINT
);

-- Materialized view — create AFTER loading data into source tables
CREATE MATERIALIZED VIEW financial_hist.historic_financial_qsreport
DISTSTYLE ALL AUTO REFRESH NO AS
SELECT
    b.bookname,
    b.booktype,
    b.fiscalgroupname,
    fe.gl_program_cd,
    fe.gp_db_name,
    fe.term_cd,
    fe.fiscal_year_label,
    fe.nbr_non_discount_students,
    fe.nbr_discount_students,
    fe."program groupings key",
    fet.revision_label,
    fet."summer head count",
    fet."fall head count",
    fet."ay head count",
    fet."spring head count",
    fet."total head count",
    fxb.currency_code,
    fxb.currency_per_usd,
    fxb.fiscal_year_id,
    fxb.begin_date,
    fxb.default_book,
    fxh.ratetype,
    fxh.curncyid,
    fxh.xchgrate,
    fxh.effxchgrate,
    fxh.exchdate,
    fxh.periodid,
    fxh.year1,
    h.period,
    h.fnccur,
    h.transaction_datetime,
    h.description_long,
    h.vendor_name,
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
    h.fiscal_year_id AS h_fiscal_year_id,
    msd."program groupings key" AS msd_program_groupings_key,
    msd.fiscal_year,
    msd.book AS msd_book,
    msd.gl_program_cd AS msd_gl_program_cd,
    msd.gp_db_name AS msd_gp_db_name,
    msd."program name",
    msd."application status",
    msd."enrollment type",
    msd.term_category,
    msd.term_cd AS msd_term_cd,
    msd.term_length,
    msd.student_term_admit_id,
    msd.student_status_type,
    pg.gl_program_name,
    pg.glprogramcategory,
    pg.incomestatement,
    pg.businessline,
    pg.programmodel,
    sfx.book AS sfx_book,
    sfx.bookindex
FROM financial_hist.historic_all_financial_transactions h
LEFT JOIN financial_hist.bookdisplaytypeindex b
    ON h.book = b.bookname
LEFT JOIN financial_hist.forecastenrollment fe
    ON h.book = fe.book
    AND h.gp_db_name = fe.gp_db_name
LEFT JOIN financial_hist.forecastenrollmenttotals fet
    ON fe.gl_program_cd = fet.gl_program_cd
    AND fe.gp_db_name = fet.gp_db_name
    AND fe.fiscal_year_label = fet.fiscal_year_label
LEFT JOIN financial_hist.fx_book_rates fxb
    ON h.fnccur = fxb.currency_code
    AND fet.revision_label = fxb.revision_label
LEFT JOIN financial_hist.fx_rates_hist fxh
    ON fxb.currency_code = fxh.curncyid
    AND h.period = fxh.periodid
LEFT JOIN financial_hist.ministudentdetailsandstatushistory msd
    ON h.book = msd.book
    AND h.gp_db_name = msd.gp_db_name
LEFT JOIN financial_hist.program_groupings pg
    ON fe.gl_program_cd = pg.gl_program_cd
    AND fe.gp_db_name = pg.gp_db_name
LEFT JOIN financial_hist.slicer_fxbookrates sfx
    ON h.book = sfx.book;

COMMENT ON SCHEMA financial_hist IS 'Dev replica of prod iesabroad_dw.financial_hist — minimum tables for historic_financial_qsreport';
