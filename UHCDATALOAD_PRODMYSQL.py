import os
import sys
sys.path.append("F:\Shares\Java_migration\common")

import datetime as dt
from datetime import datetime
import pandas as pd
from Snowflake_conn import Connection
from sqlalchemy import create_engine
from urllib.parse import quote_plus

password = quote_plus('i^Z9Z3r)')

user = "cfmp_actul_load_usr"
host = 'rp000192956'
port = 3306
database = 'cfmp01'

today_date = datetime.today().date()

#########################################################################
# Drill Date Logic
#########################################################################
def calculate_drill_date(today=None):

    if today is None:
        today = dt.date.today()

    year = today.year
    month = today.month
    day = today.day

    # Workday 7 logic (1st–12th → previous month)
    if day <= 12:
        if month == 1:
            drill_year = year - 1
            drill_month = 12
        else:
            drill_year = year
            drill_month = month - 1
    else:
        drill_year = year
        drill_month = month

    return f"{drill_year}-{drill_month:02d}-01"


# ✅ Generate drill_date once
drill_date = calculate_drill_date()
print("Calculated Drill Date:", drill_date)

#########################################################################
# Extract Data
#########################################################################
def extract():

    session = Connection()

    query1 = f"""
    SELECT
    concat('A',trim(ACCOUNT)) gl_acct,
    trim(LEDGER) lgr,
    concat('FY', year(DRILL_DATE)) fisc_yr, 
    substr('JANFEBMARAPRMAYJUNJULAUGSEPOCTNOVDEC',MONTH(drill_date)*3-2, 3) mo,
    concat('J',PROJECT_ID) ucmg_id, 
    cast(sum(AMOUNT) as DECIMAL(19,4)) amt,
    current_timestamp() insrt_on,
    trim(BUSINESS_UNIT) BU,
    trim(OPERATING_UNIT) OU,
    trim(LOCATION) LOC,
    trim(DEPTID) DEPT,
    'UHC' BUS_FLG
    FROM bpmda.vw_fdw_actuals_flipped D 
    WHERE DRILL_DATE = '{drill_date}'  
    AND BUSINESS_UNIT='20020' 
    AND OPERATING_UNIT='02858' 
    AND DEPTID='220210'
    AND LEDGER='GAAP'
    AND ACCOUNT between '40000' AND '99999'
    GROUP BY ACCOUNT, LEDGER, DRILL_DATE,PROJECT_ID,BUSINESS_UNIT,OPERATING_UNIT,LOCATION,DEPTID
    HAVING cast(sum(AMOUNT) AS DECIMAL(19,4)) != 0

    UNION ALL

    SELECT
    concat('A',trim(ACCOUNT)) gl_acct,
    trim(LEDGER) lgr,
    concat('FY', year(DRILL_DATE)) fisc_yr, 
    substr('JANFEBMARAPRMAYJUNJULAUGSEPOCTNOVDEC',MONTH(drill_date)*3-2, 3) mo,
    concat('J',PROJECT_ID) ucmg_id, 
    cast(sum(AMOUNT) as DECIMAL(19,4)) amt,
    current_timestamp() insrt_on,
    trim(BUSINESS_UNIT) BU,
    trim(OPERATING_UNIT) OU,
    trim(LOCATION) LOC,
    trim(DEPTID) DEPT,
    'UHG' BUS_FLG
    FROM bpmda.vw_fdw_actuals_flipped D 
    WHERE DRILL_DATE = '{drill_date}'  
    AND BUSINESS_UNIT='20020'
    AND OPERATING_UNIT='01000'
    AND (account between '40000' AND '99999' OR account in ('15050','15060','15055','15065'))
    AND concat('D',DEPTID) in (select leaf from tleaves where treecd = 'DEPT' and node = 'ES100')
    AND CONCAT('J',PROJECT_ID) IN (SELECT LEAF FROM TLEAVES where treecd = 'PROJ' and node = 'CRAG_CAPITAL_PROJ')
    GROUP BY ACCOUNT, LEDGER, DRILL_DATE,PROJECT_ID,BUSINESS_UNIT,OPERATING_UNIT,LOCATION,DEPTID
    HAVING cast(sum(AMOUNT) AS DECIMAL(19,4)) != 0
    """

    df1 = session.sql(query1).to_pandas()
    return df1


#########################################################################
# Load to MySQL
#########################################################################
def dataload(df, tbl):

    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

    try:
        df.to_sql(tbl, con=engine, if_exists='append', index=False, method='multi')
        print("Data loaded successfully.")

    except Exception as e:
        print("Error loading data:", e)


#########################################################################
# Execute
#########################################################################
df1 = extract()
dataload(df1, 'cig_gaap_actul_lnd_zone')
