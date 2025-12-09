from snowflake.connector import connect

ctx=connect(user='DIEGOMONTEMAYOR',password='Uwt9CbCTMy4QHA9',
            account='dj90916.us-east-2.aws',warehouse='mini_crust_wh',
            database='mini_crust_db')
q=lambda s:ctx.cursor().execute(s)

def model():
    q("CREATE SCHEMA IF NOT EXISTS RAW")
    q("CREATE SCHEMA IF NOT EXISTS STAGING")
    q("CREATE SCHEMA IF NOT EXISTS CORE")
    q("CREATE SCHEMA IF NOT EXISTS CUSTOMER")
    q("USE SCHEMA STAGING")
    q("""CREATE OR REPLACE TABLE HN_JOBS_FLAT AS
        SELECT LOWER(SPLIT_PART(LOWER(DATA:title)::string,' (yc',1)) id,
               (DATA:title)::string title,
               SPLIT_PART(LOWER(DATA:title)::string,' (yc',1) company_name,
               (DATA:time)::timestamp created_ts,
               INGEST_TS
        FROM RAW.HN_JOBS_RAW""")
    q("USE SCHEMA CORE")
    q("""CREATE OR REPLACE TABLE COMPANY AS
        SELECT * FROM (
          SELECT LOWER(payload:name::string) id,
                 payload:name::string name,
                 MIN(ingest_ts) first_seen,
                 MAX(ingest_ts) last_seen
          FROM RAW.FUNDING_EVENTS_RAW GROUP BY 1,2
          UNION ALL
          SELECT LOWER(company_name),company_name,MIN(INGEST_TS),MAX(INGEST_TS)
          FROM STAGING.HN_JOBS_FLAT GROUP BY 1,2
        )""")

def scd_lite():
    q("""CREATE TABLE IF NOT EXISTS COMPANY_SCD_LITE(
         id STRING,name STRING,hash STRING,
         valid_from TIMESTAMP,valid_to TIMESTAMP,is_current BOOLEAN)""")
    q("""CREATE OR REPLACE TEMP TABLE _new AS SELECT id,name,first_seen ingest_ts FROM COMPANY""")
    q("""CREATE OR REPLACE TEMP TABLE _h AS
         SELECT id,name,MD5(CONCAT_WS('|',id,name)) hash,ingest_ts FROM _new""")
    q("""MERGE INTO COMPANY_SCD_LITE t USING _h s
         ON t.id=s.id AND t.is_current
         WHEN MATCHED AND t.hash<>s.hash
           THEN UPDATE SET t.valid_to=s.ingest_ts,t.is_current=False
         WHEN NOT MATCHED THEN INSERT(id,name,hash,valid_from,valid_to,is_current)
           VALUES(s.id,s.name,s.hash,s.ingest_ts,'2999-12-31',True)""")

def custview():
    q("USE SCHEMA CUSTOMER")
    q("""CREATE OR REPLACE TABLE COMPANY_PROFILE AS
        WITH latest AS (
          SELECT c.id,c.name,s.hash,s.valid_from,s.valid_to,s.is_current,
                 ROW_NUMBER() OVER(PARTITION BY c.id ORDER BY s.valid_from DESC) rn
          FROM CORE.COMPANY c
          JOIN CORE.COMPANY_SCD_LITE s ON c.id=s.id
          WHERE s.is_current
        ),
        hn AS (
          SELECT LOWER(SPLIT_PART(LOWER(DATA:title)::string,' (yc',1)) id,
                 COUNT(*) cnt
          FROM RAW.HN_JOBS_RAW GROUP BY 1
        ),
        fund AS (
          SELECT LOWER(payload:name::string) id,
                 COUNT(*) cnt,
                 MAX(payload:amount_usd::number) max_amt,
                 MAX(payload:round::string) last_round
          FROM RAW.FUNDING_EVENTS_RAW GROUP BY 1
        )
        SELECT LOWER(l.name) normalized_name,
               COALESCE(h.cnt,0) hn_job_count,
               COALESCE(f.cnt,0) funding_events,
               f.max_amt,
               f.last_round,
               l.valid_from version
        FROM latest l
        LEFT JOIN hn h ON l.id=h.id
        LEFT JOIN fund f ON l.id=f.id
        WHERE l.rn=1""")
