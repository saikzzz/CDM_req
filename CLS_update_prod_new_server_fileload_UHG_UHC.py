import os
import sys
import csv
import shutil
sys.path.append("F:\Shares\Java_migration\common")
import datetime as dt
from datetime import datetime
from config import *
import pandas as pd
from Snowflake_conn import Connection
from sqlalchemy import create_engine
from urllib.parse import quote_plus

password = quote_plus('i^Z9Z3r)')

path1 = readconfig("path")
print(path1)
#path1 = "F:\Shares\Java_migration\data\csv_files"
ext = str(dt.datetime.today().strftime("%Y-%m-%d"))
print(ext)

filename =  path1+"cig_gaap_actul_lnd_zone_"+ext+".csv"
filename1 = path1+"cig_gaap_row_cnt_"+ext+".csv"
filename2 = path1+"cig_gaap_actul_idea_nbr_lnd_zone_"+ext+".csv"
user = "cfmp_actul_load_usr"
password = password
host = 'rp000192956'
port = 3306
database = 'cfmp01'

today_date = datetime.today().date()


#defining the extract data functions from snowflake to csv

def extract():
    session=Connection()
    query1="""SELECT concat('A',trim(ACCOUNT)) gl_acct,trim(LEDGER) lgr,concat('FY', year(DRILL_DATE)) fisc_yr,
substr('JANFEBMARAPRMAYJUNJULAUGSEPOCTNOVDEC',MONTH(drill_date)*3-2, 3) mo,concat('J',PROJECT_ID) ucmg_id, cast(sum(AMOUNT) as DECIMAL(19,4))amt,
TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH:MI:SS.FF3') insrt_on,trim(BUSINESS_UNIT) BU,trim(OPERATING_UNIT) OU,trim(LOCATION) LOC,trim(DEPTID) DEPT,
'UHC' BUS_FLG FROM bpmda.vw_fdw_actuals_flipped D WHERE DRILL_DATE = '2026-04-01'  AND BUSINESS_UNIT='20020'
AND OPERATING_UNIT='02858' AND DEPTID='220210'AND LEDGER='GAAP'AND ACCOUNT between '40000' AND '99999'
GROUP BY ACCOUNT, LEDGER, DRILL_DATE,PROJECT_ID,BUSINESS_UNIT,OPERATING_UNIT,LOCATION,DEPTID,BUS_FLG
HAVING cast(sum(AMOUNT) AS DECIMAL(19,4)) != 0

union all

SELECT concat('A',trim(ACCOUNT)) gl_acct,trim(LEDGER) lgr,concat('FY', year(DRILL_DATE)) fisc_yr,
substr('JANFEBMARAPRMAYJUNJULAUGSEPOCTNOVDEC',MONTH(drill_date)*3-2, 3) mo,concat('J',PROJECT_ID) ucmg_id, cast(sum(AMOUNT) as DECIMAL(19,4)) amt,
TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH:MI:SS.FF3') insrt_on,trim(BUSINESS_UNIT) BU,trim(OPERATING_UNIT) OU,trim(LOCATION) LOC,trim(DEPTID) DEPT,
'UHG' BUS_FLG FROM bpmda.vw_fdw_actuals_flipped D WHERE DRILL_DATE = '2026-04-01'  AND BUSINESS_UNIT='20020'
AND OPERATING_UNIT='01000'AND (account between '40000' AND '99999' OR account in ('15050','15060','15055' ,'15065'))
AND concat('D',DEPTID) in (select leaf from tleaves where treecd = 'DEPT' and node = 'ES100')
AND CONCAT('J',PROJECT_ID) IN (SELECT LEAF FROM TLEAVES where treecd = 'PROJ' and node = 'CRAG_CAPITAL_PROJ')
GROUP BY ACCOUNT, LEDGER,DRILL_DATE,PROJECT_ID,BUSINESS_UNIT,OPERATING_UNIT,LOCATION,DEPTID
HAVING cast(sum(AMOUNT) AS DECIMAL(19,4)) != 0 """
    df1 = session.sql(query1).to_pandas()

    query2="""SELECT TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH:MI:SS.FF3') insrt_on, COUNT(*) AS row_cnt
FROM (
    SELECT ACCOUNT, LEDGER, DRILL_DATE, PROJECT_ID, BUSINESS_UNIT, OPERATING_UNIT, LOCATION, DEPTID
    FROM bpmda.vw_fdw_actuals_flipped D
    WHERE DRILL_DATE = '2026-04-01'
      AND BUSINESS_UNIT = '20020'
      AND OPERATING_UNIT = '02858'
      AND DEPTID = '220210'
      AND LEDGER = 'GAAP'
      AND ACCOUNT BETWEEN '40000' AND '99999'
    GROUP BY ACCOUNT, LEDGER, DRILL_DATE, PROJECT_ID, BUSINESS_UNIT, OPERATING_UNIT, LOCATION, DEPTID
    HAVING CAST(SUM(AMOUNT) AS DECIMAL(19,4)) != 0
union all
select ACCOUNT, LEDGER, DRILL_DATE, PROJECT_ID, BUSINESS_UNIT, OPERATING_UNIT, LOCATION, DEPTID FROM bpmda.vw_fdw_actuals_flipped D 
WHERE DRILL_DATE = '2026-04-01'  
AND BUSINESS_UNIT='20020'
AND OPERATING_UNIT='01000'
AND (account between '40000' AND '99999' OR account in ('15050','15060','15055' ,'15065'))
AND concat('D',DEPTID) in (select leaf from tleaves where treecd = 'DEPT' and node = 'ES100')
AND CONCAT('J',PROJECT_ID) IN (SELECT LEAF FROM TLEAVES where treecd = 'PROJ' and node = 'CRAG_CAPITAL_PROJ')
GROUP BY ACCOUNT, LEDGER, DRILL_DATE,PROJECT_ID,BUSINESS_UNIT,OPERATING_UNIT,LOCATION,DEPTID
HAVING cast(sum(AMOUNT) AS DECIMAL(19,4)) != 0) sub;"""

    df2 = session.sql(query2).to_pandas()

    query3="""SELECT concat('A',trim(ACCOUNT)) gl_acct,trim(LEDGER) lgr,
concat('FY', year(DRILL_DATE)) fisc_yr,
substr('JANFEBMARAPRMAYJUNJULAUGSEPOCTNOVDEC', MONTH(drill_date)*3-2, 3) mo,
concat('J',PROJECT_ID) ucmg_id,
cast(sum(AMOUNT) as DECIMAL(19,4)) amt,
trim(line_descr) line_desc,
trim(descr) descr,
trim(oprid) opr_id,
trim(source) src,
trim(journal_id) jnl_id,
TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH:MI:SS.FF3') insrt_on,
trim(BUSINESS_UNIT) BU,
trim(OPERATING_UNIT) OU,
trim(LOCATION) LOC,
trim(DEPTID) DEPT,
'UHG' BUS_FLG,
posted_date
FROM bpmda.vw_fdw_actuals_flipped D
WHERE DRILL_DATE = '2026-04-01'
AND LEDGER='GAAP'
AND BUSINESS_UNIT='20020'
AND OPERATING_UNIT='01000'
AND (account between '40000' AND '99999' OR account in ('15050','15060','15055' ,'15065'))
AND concat('D',DEPTID) in (select leaf from tleaves where treecd = 'DEPT' and node = 'ES100')
AND CONCAT('J',PROJECT_ID) IN (SELECT LEAF FROM TLEAVES where treecd = 'PROJ' and node = 'CRAG_CAPITAL_PROJ')
GROUP BY ACCOUNT, LEDGER, DRILL_DATE, PROJECT_ID, line_descr, descr, descr254, oprid,journal_id, source,BUSINESS_UNIT,OPERATING_UNIT,LOCATION,
DEPTID,BUS_FLG,posted_date
HAVING cast(sum(AMOUNT) AS DECIMAL(19,4)) != 0

union all

SELECT concat('A',trim(ACCOUNT)) gl_acct,trim(LEDGER) lgr,
concat('FY', year(DRILL_DATE)) fisc_yr,
substr('JANFEBMARAPRMAYJUNJULAUGSEPOCTNOVDEC', MONTH(drill_date)*3-2, 3) mo,
concat('J',PROJECT_ID) ucmg_id,
cast(sum(AMOUNT) as DECIMAL(19,4)) amt,
trim(line_descr) line_desc,
trim(descr) descr,
trim(oprid) opr_id,
trim(source) src,
trim(journal_id) jnl_id,
TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH:MI:SS.FF3') insrt_on,
trim(BUSINESS_UNIT) BU,
trim(OPERATING_UNIT) OU,
trim(LOCATION) LOC,
trim(DEPTID) DEPT,
'UHC' BUS_FLG,
posted_date
FROM bpmda.vw_fdw_actuals_flipped D
WHERE DRILL_DATE = '2026-04-01'
AND BUSINESS_UNIT='20020'
AND OPERATING_UNIT='02858'
AND DEPTID='220210'
AND LEDGER='GAAP'
AND ACCOUNT between '40000' AND '99999'
GROUP BY ACCOUNT, LEDGER, DRILL_DATE, PROJECT_ID, line_descr, descr, descr254, oprid, journal_id,source,BUSINESS_UNIT,OPERATING_UNIT,LOCATION,
DEPTID,BUS_FLG,posted_date
HAVING cast(sum(AMOUNT) AS DECIMAL(19,4)) != 0"""
    df3 = session.sql(query3).to_pandas()

    
    #if today_date in df1['INSRT_ON'].dt.date.values and today_date in df2['INSRT_ON'].dt.date.values and today_date in df3['INSRT_ON'].dt.date.values:
        #df_f1 = df1
        #df_f2 = df2
        #df_f3 = df3
       
    #else:
        #sys.exit()

    return df1,df2,df3
    
    

def dataload():
    print(filename)

    df1.columns = df1.columns.str.lower()
    #df1.columns=['unit', 'oper unit','account', 'location','dept', 'journal id','user','project','line descr','year','period', 'source','amount','posted', 'reversal_cd', 'runtime', 'descr']
    df1.columns=['gl_acct', 'lgr', 'fisc_yr', 'mo', 'ucmg_id', 'amt', 'insrt_on','BU','OU','LOC','DEPT','BUS_FLG']
    df1.fillna("",inplace=True)
    df1.to_csv(filename,index =None,quoting=csv.QUOTE_ALL, quotechar='"',doublequote=True)


def dataload1():

    df2.columns = df2.columns.str.lower()
    #df1.columns=['unit', 'ROW_CNT','account', 'location','dept', 'journal id','user','project','line descr','year','period', 'source','amount','posted', 'reversal_cd', 'runtime', 'descr']
    df2.columns=['insert_on','row_cnt']
    df2.fillna("",inplace=True)
    df2.to_csv(filename1,index =None,quoting=csv.QUOTE_ALL, quotechar='"',doublequote=True)

def dataload2():

    df3.columns = df3.columns.str.lower()
    #df1.columns=['unit', 'ROW_CNT','account', 'location','dept', 'journal id','user','project','line descr','year','period', 'source','amount','posted', 'reversal_cd', 'runtime', 'descr']
    df3.columns=['gl_acct', 'lgr', 'fisc_yr', 'mo', 'ucmg_id', 'amt', 'line_desc', 'descr', 'opr_id', 'src', 'jnl_id', 'insrt_on','BU','OU','LOC','DEPT','BUS_FLG','posted_date']
    df3.fillna("",inplace=True)
    df3.to_csv(filename2,index =None,quoting=csv.QUOTE_ALL, quotechar='"',doublequote=True)    

def filecpy():
    
    #original1 = filename
    target1 = r'\\nas00262pn\Data\CDM_Shared_Folder\Dev\Peoplesoft\received'
    target2 = r'\\nas00262pn\Data\CDM_Shared_Folder\Test\Peoplesoft\received'
    target3 = r'\\nas00262pn\Data\CDM_Shared_Folder\Stage\Peoplesoft\received'
    target4 = r'\\nas00262pn\Data\CDM_Shared_Folder\Prod\Peoplesoft\received'
    #target5 = r'\\nas00262pn\Data\CDM_Shared_Folder\Dev\Peoplesoft\received'
    
    shutil.copy(filename,target1)
    shutil.copy(filename,target2)
    shutil.copy(filename,target3)
    shutil.copy(filename,target4)
    #shutil.copy(filename,target5)
    
    shutil.copy(filename1,target1)
    shutil.copy(filename1,target2)
    shutil.copy(filename1,target3)
    shutil.copy(filename1,target4)
    #shutil.copy(filename1,target5)
    
    shutil.copy(filename2,target1)
    shutil.copy(filename2,target2)
    shutil.copy(filename2,target3)
    shutil.copy(filename2,target4)
    #shutil.copy(filename2,target5)
    
    
# calling functions below

df1,df2,df3 = extract()
dataload()
dataload1()
dataload2()
filecpy()


