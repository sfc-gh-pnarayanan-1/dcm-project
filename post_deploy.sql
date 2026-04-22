--------------------------------------------------------------------
-- POST-DEPLOY COMPANION SCRIPT
-- Run this AFTER DCM deploy completes
-- Contains: Procedures, UDTFs, sequences, transient tables, views
--           referencing external objects — objects not supported by
--           DEFINE statements in DCM
--------------------------------------------------------------------

--------------------------------------------------------------------
-- SEQUENCES
--------------------------------------------------------------------

CREATE SEQUENCE IF NOT EXISTS DEMO.RESTRO.MY_UNIQUE_ID_SEQ
    START WITH 1
    INCREMENT BY 1
    NOORDER;

--------------------------------------------------------------------
-- TRANSIENT TABLES (DCM does not support TRANSIENT keyword)
--------------------------------------------------------------------

CREATE TRANSIENT TABLE IF NOT EXISTS DEMO.RESTRO.DATA_BATCH_JSON (
    BILL_JSON VARIANT
);

--------------------------------------------------------------------
-- UDTFs — DT_DEMO schema
--------------------------------------------------------------------

CREATE OR REPLACE FUNCTION DEMO.DT_DEMO.GEN_CUST_INFO("NUM_RECORDS" NUMBER(38,0))
RETURNS TABLE ("CUSTID" NUMBER(10,0), "CNAME" VARCHAR(100), "SPENDLIMIT" NUMBER(10,2))
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('Faker')
HANDLER = 'CustTab'
AS '
from faker import Faker
import random

fake = Faker()

class CustTab:
    def process(self, num_records):
        customer_id = 1000
        for _ in range(num_records):
            custid = customer_id + 1
            cname = fake.name()
            spendlimit = round(random.uniform(1000, 10000),2)
            customer_id += 1
            yield (custid,cname,spendlimit)
';

CREATE OR REPLACE FUNCTION DEMO.DT_DEMO.GEN_CUST_PURCHASE("NUM_RECORDS" NUMBER(38,0), "NDAYS" NUMBER(38,0))
RETURNS TABLE ("CUSTID" NUMBER(10,0), "PURCHASE" VARIANT)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('Faker')
HANDLER = 'genCustPurchase'
AS '
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

class genCustPurchase:
    def process(self, num_records, ndays):
        for _ in range(num_records):
            c_id = fake.random_int(min=1001, max=1999)
            current_date = datetime.now()
            min_date = current_date - timedelta(days=ndays)
            pdate = fake.date_between_dates(min_date, current_date)
            purchase = {
                ''prodid'': fake.random_int(min=101, max=199),
                ''quantity'': fake.random_int(min=1, max=5),
                ''purchase_amount'': round(random.uniform(10, 1000),2),
                ''purchase_date'': pdate
            }
            yield (c_id, purchase)
';

CREATE OR REPLACE FUNCTION DEMO.DT_DEMO.GEN_PROD_INV("NUM_RECORDS" NUMBER(38,0))
RETURNS TABLE ("PID" NUMBER(10,0), "PNAME" VARCHAR(100), "STOCK" NUMBER(10,2), "STOCKDATE" DATE)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('Faker')
HANDLER = 'ProdTab'
AS '
from faker import Faker
import random
from datetime import datetime, timedelta
fake = Faker()

class ProdTab:
    def process(self, num_records):
        product_id = 100
        for _ in range(num_records):
            pid = product_id + 1
            pname = fake.catch_phrase()
            stock = round(random.uniform(500, 1000),0)
            current_date = datetime.now()
            min_date = current_date - timedelta(days=90)
            stockdate = fake.date_between_dates(min_date, current_date)
            product_id += 1
            yield (pid, pname, stock, stockdate)
';

--------------------------------------------------------------------
-- UDTFs — RESTRO schema
--------------------------------------------------------------------

CREATE OR REPLACE FUNCTION DEMO.RESTRO.GEN_CUST_PURCHASE("NUM_RECORDS" NUMBER(38,0), "NDAYS" NUMBER(38,0))
RETURNS TABLE ("CUSTID" NUMBER(10,0), "PURCHASE" VARIANT, "CREATED_ON" TIMESTAMP_NTZ(9))
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('Faker')
HANDLER = 'genCustPurchase'
AS '
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

class genCustPurchase:
    def process(self, num_records, ndays):
        for _ in range(num_records):
            c_id = fake.random_int(min=1001, max=1999)
            current_date = datetime.now()
            min_date = current_date - timedelta(days=ndays)
            pdate = fake.date_between_dates(min_date, current_date)
            created_timestamp = fake.date_time_between_dates(datetime_start=min_date, datetime_end=current_date)
            purchase = {
                ''prodid'': fake.random_int(min=101, max=199),
                ''quantity'': fake.random_int(min=1, max=5),
                ''purchase_amount'': round(random.uniform(10, 1000),2),
                ''purchase_date'': pdate
            }
            yield (c_id, purchase, created_timestamp)
';

--------------------------------------------------------------------
-- PROCEDURES — PUBLIC schema
--------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE DEMO.PUBLIC.SIMPLE_FOR("ITERATION_LIMIT" NUMBER(38,0))
RETURNS NUMBER(38,0)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
    DECLARE
        counter INTEGER DEFAULT 0;
    BEGIN
        FOR i IN 1 TO iteration_limit DO
            i := i + 9;
            counter := i;
        END FOR;
        RETURN counter;
    END;
';

CREATE OR REPLACE PROCEDURE DEMO.PUBLIC.CALL_CORTEX_AGENT(
    "PROMPT" VARCHAR,
    "AGENT_DATABASE" VARCHAR DEFAULT ''cortex_analyst_demo'',
    "AGENT_SCHEMA" VARCHAR DEFAULT ''cortex_agent'',
    "AGENT_NAME" VARCHAR DEFAULT ''MASTER_AGENT'',
    "MODEL" VARCHAR DEFAULT ''claude-3-5-sonnet''
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python','requests')
HANDLER = 'call_agent'
EXTERNAL_ACCESS_INTEGRATIONS = (CORTEX_API_ACCESS)
EXECUTE AS CALLER
AS $$
import _snowflake
import requests
import json

def call_agent(session, prompt, agent_database, agent_schema, agent_name, model):
    account_info = session.sql("SELECT CURRENT_ACCOUNT(), CURRENT_REGION()").collect()[0]
    account = account_info[0]
    region = account_info[1]
    host = f"{account.lower()}.{region.lower()}.snowflakecomputing.com"
    token = _snowflake.get_generic_secret_string('token')
    url = f"https://{host}/api/v2/databases/{agent_database}/schemas/{agent_schema}/agents/{agent_name}:run"
    headers = {
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        "Authorization": f"Bearer {token}",
        "X-Snowflake-Authorization-Token-Type": "OAUTH"
    }
    body = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        "model": model
    }
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=120)
        if resp.status_code != 200:
            return {"success": False, "error": f"HTTP {resp.status_code}", "response": resp.text[:1000]}
        events = parse_sse(resp.text)
        return {"success": True, "events": events}
    except Exception as e:
        return {"success": False, "error": str(e)}

def parse_sse(response_text):
    events = []
    current_event = {}
    for line in response_text.split('\n'):
        line = line.strip()
        if not line:
            if current_event:
                events.append(current_event)
                current_event = {}
            continue
        if line.startswith('event:'):
            current_event['event'] = line[6:].strip()
        elif line.startswith('data:'):
            try:
                current_event['data'] = json.loads(line[5:].strip())
            except:
                current_event['data'] = line[5:].strip()
    if current_event:
        events.append(current_event)
    return events
$$;

--------------------------------------------------------------------
-- PROCEDURES — RESTRO schema
-- NOTE: JSON_FLATTEN and PROCESS_POSIST_BILLS are large ETL
-- procedures. Deploy from their original source files or
-- run GET_DDL('PROCEDURE', ...) to extract them.
--------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE DEMO.RESTRO.LOAD_DAILY_PARTITIONED_DATA_PY("SOURCE_TABLE_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'load_data'
EXECUTE AS OWNER
AS $$
def load_data(session, source_table_name):
    from datetime import datetime, timedelta
    query = f"SELECT MIN(created_on) as min_date, MAX(created_on) as max_date FROM {source_table_name}"
    result = session.sql(query).collect()
    min_date = result[0]['MIN_DATE'].date()
    max_date = result[0]['MAX_DATE'].date()
    total_rows = 0
    batch_count = 0
    current_date = min_date
    while current_date <= max_date:
        date_str = current_date.strftime('%Y-%m-%d')
        insert_query = f"""
            INSERT INTO customer_purchase_partitioned
            SELECT custid, purchase, created_on
            FROM {source_table_name}
            WHERE DATE(created_on) = '{date_str}'
            ORDER BY created_on, custid
        """
        session.sql(insert_query).collect()
        count_query = f"SELECT COUNT(*) as cnt FROM customer_purchase_partitioned WHERE DATE(created_on) = '{date_str}'"
        count_result = session.sql(count_query).collect()
        rows_inserted = count_result[0]['CNT']
        total_rows += rows_inserted
        batch_count += 1
        current_date = current_date + timedelta(days=1)
    return f"Completed: Inserted {total_rows} rows across {batch_count} daily batches from {min_date} to {max_date}"
$$;

--------------------------------------------------------------------
-- NOTE: The following RESTRO procedures are very large ETL scripts.
-- They are NOT included inline here to keep this file maintainable.
-- To deploy them, use GET_DDL() from the source environment:
--
--   SELECT GET_DDL('PROCEDURE', 'DEMO.RESTRO.JSON_FLATTEN()');
--   SELECT GET_DDL('PROCEDURE', 'DEMO.RESTRO.PROCESS_POSIST_BILLS(...)');
--   SELECT GET_DDL('PROCEDURE', 'DEMO.RESTRO.PROCESS_POSIST_BILLS_MAIN(NUMBER)');
--------------------------------------------------------------------

--------------------------------------------------------------------
-- VIEWS referencing external table functions (not DCM-compatible)
-- AI_MULTI_AGENT_TRACES_VIEW references SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS
--------------------------------------------------------------------

-- NOTE: DEMO.PUBLIC.AI_MULTI_AGENT_TRACES_VIEW uses
-- TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(...))
-- which is an account-level system function. Deploy this view
-- manually or uncomment below after verifying the referenced
-- objects exist in the target environment.
-- To extract: SELECT GET_DDL('VIEW', 'DEMO.PUBLIC.AI_MULTI_AGENT_TRACES_VIEW');
