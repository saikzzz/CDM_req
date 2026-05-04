import os
import sys
sys.path.append("F:\Shares\Java_migration\common")

import datetime as dt
import pandas as pd
from sqlconn import *
from config import *
import time as tm
import platform as pl
from inspect import currentframe
from utilities import *
import csv
import shutil
from Snowflake_conn import *

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


#########################################################################
# Process extraction and loading of the data
#########################################################################
def extract_and_load(querylist):
    global count
    global newmax
    try:
        for query, filename in querylist:
            extdata = srcconxn.sql(query).to_pandas()
            prlog(log, "Data extracted successfully")
    except Exception as e:
        print(e)
        msg = getattr(e, 'message', str(e))
        line = currentframe().f_lineno
        now = dt.datetime.now()
        time = now.strftime("%Y-%m-%d %H:%M:%S")
        meth = sys._getframe().f_code.co_name
        appname = str(pl.node()) + ":" + path + ":" + meth
        prlog(log, msg + " error while extracting the data at " + meth + " method")
        mails = ["rk_g@optum.com"]
        send_email(mails, error, msg)
        eject(time, appname, line, msg)

    try:
        extdata.columns = extdata.columns.str.lower()
        extdata.columns = ['unit', 'oper unit', 'account', 'location', 'dept', 'journal id',
                           'user', 'project', 'line descr', 'year', 'period', 'source',
                           'amount', 'posted', 'reversal_cd', 'runtime', 'descr']
        extdata.fillna("", inplace=True)
        extdata.to_csv(filename, index=None, quoting=csv.QUOTE_ALL, quotechar='"', doublequote=True)

        count = extdata.shape[0]
        newmax = extdata["posted"].max()

        msg = f"CDM Optum Peoplesoft extract is finished. {count} rows of data were produced."
        prlog(log, msg)

        rowcount = "Rowcount: " + str(count)
        now = dt.datetime.now()
        time = now.strftime("%Y-%m-%d %H:%M:%S")
        meth = sys._getframe().f_code.co_name
        appname = str(pl.node()) + ":" + path + ":" + meth

        rc = (time, loglevel, logclass, appname, currentframe().f_lineno, rowcount)
        rowquery = """insert into Log(TimeStamp, LogLevel, LogClass, AppName,LineNum,Message) VALUES {0}""".format(tuple(rc))
        execute(logdb, rowquery)

        prlog(log, "Log table updated successfully")
        prlog(log, "Data loaded to file successfully")

    except Exception as e:
        msg = getattr(e, 'message', str(e))
        line = currentframe().f_lineno
        now = dt.datetime.now()
        time = now.strftime("%Y-%m-%d %H:%M:%S")
        meth = sys._getframe().f_code.co_name
        appname = str(pl.node()) + ":" + path + ":" + meth
        prlog(log, msg + " error while loading the data")
        mails = ["rk_g@optum.com"]
        send_email(mails, error, msg)
        eject(time, appname, line, msg)


#########################################################################
# Queries
#########################################################################
def queries():
    sqlquery = []

    sqlquery.append([
        " SELECT business_unit, operating_unit , account, Location, deptid Dept, journal_id , " +
        " CASE WHEN c.employeename IS NULL THEN a.oprid ELSE c.employeename END " +
        " , project_id Project, replace(line_descr, '\"', ' '), year(drill_date) Year, month(drill_date) " +
        " Period, Source, sum(Amount) Amount, posted_date, reversal_cd, current_timestamp() Runtime, descr " +
        " FROM optum.vw_fdw_actuals a join tleaves b on concat('S',a.operating_unit)=b.leaf and b.node='OPTUM MG' " +
        " left join bpmda.operator c on a.oprid=c.employeeid " +
        " WHERE ledger='GAAP' AND drill_date='" + str(drilldate) + "' AND project_id like '1%' AND posted_date <= '" + str(maxpd) + "' " +
        " GROUP BY business_unit, operating_unit, account, Location, deptid, journal_id, project_id, line_descr, " +
        " drill_date, Source, posted_date, reversal_cd, current_timestamp(), descr " +
        " , CASE WHEN c.employeename IS NULL THEN a.oprid ELSE c.employeename END",
        filename
    ])

    return sqlquery


#########################################################################
# Params
#########################################################################
def params():
    global srcconxn, logdb, loglevel, logclass, watchlist
    global path, status, error, log, drilldate, filename, maxpd

    path = os.getcwd().split('\\')[-2:]
    path = ".".join(path) + ".cdmextract.py"

    loglevel = "Outline"
    logclass = "DataETL"
    status = "Status of CDMExtract job"
    error = "Error..! CDMExtract job finished with Errors"

    log = open("CDMExtract.txt", "w")

    try:
        watchlist = readconfig('cdm_emails').split(',')

        # ✅ UPDATED HERE
        drilldate = calculate_drill_date()

        srcconxn = cdm_opeator()

        file = readconfig("cdmextract")
        filepath = readconfig("path")
        filename = filepath + file + str(dt.datetime.today().strftime("%Y-%m-%d")) + "_snowflake.csv"

        logdb = readconfig("logdb")

    except Exception as e:
        msg = getattr(e, 'message', str(e))
        prlog(log, msg)
        send_email(["rk_g@optum.com"], error, msg)
        raise

    try:
        query = "SELECT max(posted_date) from bpmda.cdmoptum_posteddates"
        df = srcconxn.sql(query).to_pandas()

        maxpd = df.iloc[0, 0] if df.iloc[0, 0] else "1900-01-01"

        print("Drill Date:", drilldate)
        print("Max Posted Date:", maxpd)

    except Exception as e:
        msg = getattr(e, 'message', str(e))
        prlog(log, "Max posted date retrieve error: " + msg)
        send_email(["rk_g@optum.com"], error, msg)
        raise


#########################################################################
# Main
#########################################################################
def start():
    sttime = dt.datetime.now()

    config()
    datasrc()
    params()

    prlog(log, "Process Started")

    querylist = queries()
    extract_and_load(querylist)

    shutil.copy(filename, r'\\nasv0506\optumcdmstage\PeopleSoftData\Prod\received')
    shutil.copy(filename, r'\\nasv0506\optumcdmstage\PeopleSoftData\Stage\received')

    endtime = dt.datetime.now()

    msg = f"CDM Extract finished. {count} rows processed."
    send_email(watchlist, status, msg)

    prlog(log, "Completed in " + str(endtime - sttime))
    log.close()


#########################################################################
# Run
#########################################################################
start()
