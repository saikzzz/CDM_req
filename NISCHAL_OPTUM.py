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

ext = str(dt.datetime.today().strftime("%Y-%m-%d"))
print(ext)

filename = path1 + "optum_cig_gaap_actul_lnd_zone_" + ext + ".csv"
filename1 = path1 + "optum_cig_gaap_row_cnt_" + ext + ".csv"
filename2 = path1 + "optum_cig_gaap_actul_idea_nbr_lnd_zone_" + ext + ".csv"

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

    # 1st to 12th → previous month
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


# ✅ Generate once and reuse
drill_date = calculate_drill_date()
print("Calculated Drill Date:", drill_date)


#########################################################################
# Extract Data
#########################################################################
def extract():

    session = Connection()

    query1 = f"""
    SELECT concat('A',account) gl_acct , ledger lgr ,
    concat('FY',year(drill_date)) fisc_yr ,
    substring('JanFebMarAprMayJunJulAugSepOctNovDec',month(drill_date)*3-2, 3) mo,
    concat('J',project_id) ucmg_id ,
    sum(Amount) Amt,
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH:MI:SS.FF3') insrt_on,
    business_unit unit,
    operating_unit operunit,
    Location,
    deptid Dept,
    reversal_cd,
    CASE WHEN c.employeename IS NULL THEN a.oprid ELSE c.employeename END,
    journal_id,
    posted_date
    FROM optum.vw_fdw_actuals a
    join tleaves b on concat('S',a.operating_unit)=b.leaf and b.node='OPTUM MG'
    left join bpmda.operator c on a.oprid=c.employeeid
    WHERE ledger = 'GAAP'
    AND drill_date = '{drill_date}'
    AND project_id like '1%'
    GROUP BY account,ledger,drill_date,project_id,insrt_on,unit,operunit,Location,
    Dept,reversal_cd,journal_id,posted_date,
    CASE WHEN c.employeename IS NULL THEN a.oprid ELSE c.employeename END
    """

    df1 = session.sql(query1).to_pandas()

    query2 = "select * from bpmda.cig_gaap_row_cnt"
    df2 = session.sql(query2).to_pandas()

    query3 = f"""
    SELECT concat('A',account) gl_acct , ledger lgr ,
    concat('FY',year(drill_date)) fisc_yr ,
    substring('JanFebMarAprMayJunJulAugSepOctNovDec',month(drill_date)*3-2, 3) mo,
    concat('J',project_id) ucmg_id ,
    sum(Amount) Amt,
    replace(line_descr, '\"', ' ') line_desc,
    descr,
    CASE WHEN c.employeename IS NULL THEN a.oprid ELSE c.employeename END,
    Source src,
    journal_id jnl_id ,
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH:MI:SS.FF3') insrt_on,
    business_unit unit,
    operating_unit operunit,
    Location,
    deptid Dept,
    posted_date,
    reversal_cd
    FROM optum.vw_fdw_actuals a
    join tleaves b on concat('S',a.operating_unit)=b.leaf and b.node='OPTUM MG'
    left join bpmda.operator c on a.oprid=c.employeeid
    WHERE ledger = 'GAAP'
    AND drill_date = '{drill_date}'
    AND project_id like '1%'
    GROUP BY account,ledger,drill_date,project_id,line_descr,descr,oprid,
    Source,journal_id,insrt_on,unit,operunit,Location,Dept,
    posted_date,reversal_cd,
    CASE WHEN c.employeename IS NULL THEN a.oprid ELSE c.employeename END
    """

    df3 = session.sql(query3).to_pandas()

    return df1, df2, df3


#########################################################################
# Load CSVs
#########################################################################
def dataload():

    print(filename)

    df1.columns = df1.columns.str.lower()
    df1.columns = [
        'gl_acct','lgr','fisc_yr','mo','ucmg_id','amt','insrt_on',
        'unit','oper unit','location','dept','reversal_cd',
        'user','journal_id','posted_date'
    ]

    df1.fillna("", inplace=True)

    df1.to_csv(
        filename,
        index=None,
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        doublequote=True
    )


def dataload1():

    df2.columns = df2.columns.str.lower()
    df2.columns = ['insert_on','row_cnt']

    df2.fillna("", inplace=True)

    df2.to_csv(
        filename1,
        index=None,
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        doublequote=True
    )


def dataload2():

    print(filename2)

    df3.columns = df3.columns.str.lower()

    df3.columns = [
        'gl_acct','lgr','fisc_yr','mo','ucmg_id','amt',
        'line_desc','descr','user','src','jnl_id','insrt_on',
        'unit','oper unit','location','dept',
        'posted_date','reversal_cd'
    ]

    df3.fillna("", inplace=True)

    df3.to_csv(
        filename2,
        index=None,
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        doublequote=True
    )


#########################################################################
# Copy Files
#########################################################################
def filecpy():

    target1 = r'\\nas00262pn\Data\CDM_Shared_Folder\Dev\Peoplesoft\received'
    target2 = r'\\nas00262pn\Data\CDM_Shared_Folder\Test\Peoplesoft\received'
    target3 = r'\\nas00262pn\Data\CDM_Shared_Folder\Stage\Peoplesoft\received'
    target4 = r'\\nas00262pn\Data\CDM_Shared_Folder\Prod\Peoplesoft\received'

    for f in [filename, filename1, filename2]:
        shutil.copy(f, target1)
        shutil.copy(f, target2)
        shutil.copy(f, target3)
        shutil.copy(f, target4)


#########################################################################
# Execute
#########################################################################
df1, df2, df3 = extract()
dataload()
dataload1()
dataload2()
filecpy()
