-- 6 tables for sales ETL pipeline (matches instructor lab schema)
-- Run in Redshift Query Editor v2 after workgroup is ready

-- ---------------------------------------------------------------------------
-- JOB 1: raw sales load (stage = temp each run, target = permanent UPSERT)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.stage_sales (
    uuid            BIGINT,
    country         VARCHAR(100),
    item_type       VARCHAR(50),
    sales_channel   VARCHAR(20),
    order_priority  VARCHAR(5),
    order_date      VARCHAR(20),
    region          VARCHAR(100),
    ship_date       VARCHAR(20),
    units_sold      INTEGER,
    unit_price      DECIMAL(10, 2),
    unit_cost       DECIMAL(10, 2),
    total_revenue   DECIMAL(15, 2),
    total_cost      DECIMAL(15, 2),
    total_profit    DECIMAL(15, 2)
);

CREATE TABLE IF NOT EXISTS public.target_sales (
    uuid            BIGINT,
    country         VARCHAR(100),
    item_type       VARCHAR(50),
    sales_channel   VARCHAR(20),
    order_priority  VARCHAR(5),
    order_date      VARCHAR(20),
    region          VARCHAR(100),
    ship_date       VARCHAR(20),
    units_sold      INTEGER,
    unit_price      DECIMAL(10, 2),
    unit_cost       DECIMAL(10, 2),
    total_revenue   DECIMAL(15, 2),
    total_cost      DECIMAL(15, 2),
    total_profit    DECIMAL(15, 2)
);

-- ---------------------------------------------------------------------------
-- JOB 2: aggregate by region (7 rows expected)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.stage_sales_by_region (
    region            VARCHAR(100),
    total_revenue     DECIMAL(18, 2),
    total_cost        DECIMAL(18, 2),
    total_profit      DECIMAL(18, 2),
    total_units_sold  BIGINT,
    order_count       BIGINT
);

CREATE TABLE IF NOT EXISTS public.target_sales_by_region (
    region            VARCHAR(100),
    total_revenue     DECIMAL(18, 2),
    total_cost        DECIMAL(18, 2),
    total_profit      DECIMAL(18, 2),
    total_units_sold  BIGINT,
    order_count       BIGINT
);

-- ---------------------------------------------------------------------------
-- JOB 3: aggregate by country + item (736 rows expected)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.stage_sales_by_country_item (
    country           VARCHAR(100),
    item_type         VARCHAR(50),
    total_revenue     DECIMAL(18, 2),
    total_cost        DECIMAL(18, 2),
    total_profit      DECIMAL(18, 2),
    total_units_sold  BIGINT,
    order_count       BIGINT
);

CREATE TABLE IF NOT EXISTS public.target_sales_by_country_item (
    country           VARCHAR(100),
    item_type         VARCHAR(50),
    total_revenue     DECIMAL(18, 2),
    total_cost        DECIMAL(18, 2),
    total_profit      DECIMAL(18, 2),
    total_units_sold  BIGINT,
    order_count       BIGINT
);

-- Verify (should return 6 rows)
SELECT tablename FROM pg_tables
WHERE schemaname = 'public'
  AND tablename LIKE '%sales%'
ORDER BY tablename;
