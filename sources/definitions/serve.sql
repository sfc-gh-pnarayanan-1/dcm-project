DEFINE VIEW DEMO{{env_suffix}}.DT_DEMO.V_CUSTOMER_SALES (
    CUSTOMER_ID,
    CUSTOMER_NAME,
    TOTAL_SALES_AMOUNT
) AS
    SELECT DISTINCT
        a.customer_id,
        a.customer_name,
        b.saleprice * b.quantity AS total_sales_amount
    FROM DEMO{{env_suffix}}.DT_DEMO.V_SALESREPORT a,
         DEMO{{env_suffix}}.DT_DEMO.CUSTOMER_SALES_DATA_HISTORY b
    WHERE a.customer_id = b.customer_id;

DEFINE VIEW DEMO{{env_suffix}}.DT_DEMO.V_CUST_INFO (
    CUSTID,
    CNAME,
    SPENDLIMIT
) AS
    SELECT * FROM DEMO{{env_suffix}}.DT_DEMO.CUST_INFO;

DEFINE VIEW DEMO{{env_suffix}}.DT_DEMO.V_CUST_SALES (
    CUSTID,
    PURCHASE
) AS
    SELECT a.custid, b.purchase
    FROM DEMO{{env_suffix}}.DT_DEMO.CUST_INFO a,
         DEMO{{env_suffix}}.DT_DEMO.SALESDATA b
    WHERE a.custid = b.custid;

DEFINE VIEW DEMO{{env_suffix}}.DT_DEMO.V_PROD_STOCK_INV (
    PID,
    PNAME,
    STOCK,
    STOCKDATE
) AS
    SELECT * FROM DEMO{{env_suffix}}.DT_DEMO.PROD_STOCK_INV;

DEFINE VIEW DEMO{{env_suffix}}.DT_DEMO.V_SALESREPORT (
    CUSTOMER_ID,
    CUSTOMER_NAME,
    PRODUCT_ID,
    PRODUCT_NAME,
    SALEPRICE,
    QUANTITY,
    UNITSALESPRICE,
    CREATIONTIME,
    CUSTOMER_SK,
    END_TIME
) AS
    SELECT * FROM DEMO{{env_suffix}}.DT_DEMO.SALESREPORT;

DEFINE VIEW DEMO{{env_suffix}}.DT_DEMO.V_SALES_DATE (
    CUSTID,
    PURCHASE
) AS
    SELECT * FROM DEMO{{env_suffix}}.DT_DEMO.SALESDATA;

DEFINE VIEW DEMO{{env_suffix}}.PUBLIC.SUPPLIER (
    NATION_KEY,
    SUPPLIER_COUNT,
    AVG_BALANCE,
    TOTAL_BALANCE
) AS
    SELECT
        s_nationkey AS nation_key,
        COUNT(*) AS supplier_count,
        AVG(s_acctbal) AS avg_balance,
        SUM(s_acctbal) AS total_balance
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.SUPPLIER
    GROUP BY s_nationkey
    ORDER BY supplier_count DESC;
