DEFINE DYNAMIC TABLE DEMO{{env_suffix}}.DT_DEMO.CUSTOMER_SALES_DATA_HISTORY (
    CUSTOMER_ID,
    CUSTOMER_NAME,
    PRODUCT_ID,
    SALEPRICE,
    QUANTITY,
    SALESDATE
)
    TARGET_LAG = 'DOWNSTREAM'
    REFRESH_MODE = AUTO
    INITIALIZE = ON_CREATE
    WAREHOUSE = XSMALL_WH
AS
    SELECT
        s.custid AS customer_id,
        c.cname AS customer_name,
        s.purchase:"prodid"::NUMBER(5) AS product_id,
        s.purchase:"purchase_amount"::NUMBER(10) AS saleprice,
        s.purchase:"quantity"::NUMBER(5) AS quantity,
        s.purchase:"purchase_date"::DATE AS salesdate
    FROM
        DEMO{{env_suffix}}.DT_DEMO.CUST_INFO c
        INNER JOIN DEMO{{env_suffix}}.DT_DEMO.SALESDATA s ON c.custid = s.custid;

DEFINE DYNAMIC TABLE DEMO{{env_suffix}}.DT_DEMO.SALESREPORT (
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
)
    TARGET_LAG = '1 minute'
    REFRESH_MODE = AUTO
    INITIALIZE = ON_CREATE
    WAREHOUSE = XSMALL_WH
AS
    SELECT
        t1.customer_id,
        t1.customer_name,
        t1.product_id,
        p.pname AS product_name,
        t1.saleprice,
        t1.quantity,
        (t1.saleprice / t1.quantity) AS unitsalesprice,
        t1.salesdate AS CreationTime,
        t1.customer_id || '-' || t1.product_id || '-' || t1.salesdate AS CUSTOMER_SK,
        LEAD(CreationTime) OVER (PARTITION BY t1.customer_id ORDER BY CreationTime ASC) AS END_TIME
    FROM
        DEMO{{env_suffix}}.DT_DEMO.CUSTOMER_SALES_DATA_HISTORY t1
        INNER JOIN DEMO{{env_suffix}}.DT_DEMO.PROD_STOCK_INV p ON t1.product_id = p.pid;
