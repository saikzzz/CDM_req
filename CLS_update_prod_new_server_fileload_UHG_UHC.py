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
    """
    Business Rule:

    Workday -6  -> around 20th of current month
    Workday 7   -> around 12th of next month

    Final Rule Implemented:

    1st to 12th   -> Previous Month
    13th onward   -> Current Month

    Example:
    Apr 20 to May 12 -> 2026-04-01
    May 13 onward    -> 2026-05-01
    Jan 1 to Jan 12  -> Previous year Dec
    """

    if today is None:
        today = dt.date.today()

    year = today.year
    month = today.month
    day = today.day

    # 1st to 12th = previous month
    if day <= 12:
        if month == 1:
            drill_year = year - 1
            drill_month = 12
        else:
            drill_year = year
            drill_month = month - 1

    # 13th onward = current month
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

        extdata.columns = [
            'unit', 'oper unit', 'account', 'location', 'dept',
            'journal id', 'user', 'project', 'line descr',
            'year', 'period', 'source', 'amount', 'posted',
            'reversal_cd', 'runtime', 'descr'
        ]

        extdata.fillna("", inplace=True)

        extdata.to_csv(
            filename,
            index=None,
            quoting=csv.QUOTE_ALL,
            quotechar='"',
            doublequote=True
        )

        count = extdata.shape[0]
        newmax = extdata["posted"].max()

        print(newmax)

        msg = f"CDM Optum Peoplesoft extract is finished. {count} rows of data were produced."
        prlog(log, msg)

        rowcount = "Rowcount: " + str(count)

        now = dt.datetime.now()
        time = now.strftime("%Y-%m-%d %H:%M:%S")
        meth = sys._getframe().f_code.co_name
        appname = str(pl.node()) + ":" + path + ":" + meth

        rc = (time, loglevel, logclass, appname, currentframe().f_lineno, rowcount)

        rowquery = """
        insert into Log
        (TimeStamp, LogLevel, LogClass, AppName, LineNum, Message)
        VALUES {0}
        """.format(tuple(rc))

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
        prlog(log, msg + " error while loading the data to file at " + meth + " method")
        mails = ["rk_g@optum.com"]
        send_email(mails, error, msg)
        eject(time, appname, line, msg)


#########################################################################
# Wait for completion of dependencies
#########################################################################
def jobupline(jobnames):

    joblist = "'" + jobnames[0] + "'"
    check = len(jobnames)

    try:
        if check > 1:
            for i in range(1, len(jobnames)):
                joblist = joblist + ",'" + jobnames[i] + "'"

        query = """
        select count(*)
        from bpmda.bpmjoblog
        where job in ({0})
        and status = 0
        """.format(joblist)

        df = srcconxn.sql(query).to_pandas()
        result = df.iloc[0, 0]

        if result == check:
            prlog(log, "All dependencies completed")

        else:
            while True:
                df = srcconxn.sql(query).to_pandas()
                result = df.iloc[0, 0]

                if result == check:
                    prlog(log, "All dependencies completed")
                    break

                prlog(log, "Waiting for " + joblist + " to finish")
                tm.sleep(60)

    except Exception as e:
        msg = getattr(e, 'message', str(e))
        line = currentframe().f_lineno
        now = dt.datetime.now()
        time = now.strftime("%Y-%m-%d %H:%M:%S")
        meth = sys._getframe().f_code.co_name
        appname = str(pl.node()) + ":" + path + ":" + meth
        prlog(log, msg + " error while completing dependencies")
        mails = ["rk_g@optum.com"]
        send_email(mails, error, msg)
        eject(time, appname, line, msg)


#########################################################################
# Queries
#########################################################################
def queries():

    sqlquery = []

    sqlquery.append([
        " SELECT business_unit, operating_unit, account, location, deptid Dept, journal_id, "
        " CASE WHEN c.employeename IS NULL THEN a.oprid ELSE c.employeename END, "
        " project_id Project, replace(line_descr, '\"', ' '), "
        " year(drill_date) Year, month(drill_date) Period, "
        " Source, sum(Amount) Amount, posted_date, reversal_cd, "
        " current_timestamp() Runtime, descr "
        " FROM optum.vw_fdw_actuals a "
        " join tleaves b on concat('S',a.operating_unit)=b.leaf "
        " and b.node='OPTUM MG' "
        " left join bpmda.operator c on a.oprid=c.employeeid "
        " WHERE ledger='GAAP' "
        " AND drill_date='" + str(drilldate) + "' "
        " AND project_id like '1%' "
        " AND posted_date <= '" + str(maxpd) + "' "
        " GROUP BY business_unit, operating_unit, account, location, deptid, "
        " journal_id, project_id, line_descr, drill_date, Source, "
        " posted_date, reversal_cd, current_timestamp(), descr, "
        " CASE WHEN c.employeename IS NULL THEN a.oprid ELSE c.employeename END",
        filename
    ])

    return sqlquery


#########################################################################
# Parameters
#########################################################################
def params():

    global srcconxn
    global logdb
    global actlfile
    global fcstfile
    global loc
    global jobname
    global loglevel
    global logclass
    global watchlist
    global path
    global status
    global error
    global log
    global drilldate
    global filename
    global maxpd

    path = os.getcwd().split('\\')[-2:]
    path = ".".join(path)
    path = path + ".cdmextract.py"

    loglevel = "Outline"
    logclass = "DataETL"

    status = "Status of CDMExtract job"
    error = "Error..! CDMExtract job finished with Errors"

    log = open("CDMExtract.txt", "w")

    try:
        watchlist = readconfig('cdm_emails').split(',')

        # UPDATED LOGIC HERE
        drilldate = calculate_drill_date()

        srcconxn = cdm_opeator()

        file = readconfig("cdmextract")
        filepath = readconfig("path")

        filename = (
            filepath
            + file
            + str(dt.datetime.today().strftime("%Y-%m-%d"))
            + "_snowflake.csv"
        )

        logdb = readconfig("logdb")

        print("Calculated Drill Date:", drilldate)

    except Exception as e:
        msg = getattr(e, 'message', str(e))
        line = currentframe().f_lineno
        now = dt.datetime.now()
        time = now.strftime("%Y-%m-%d %H:%M:%S")
        meth = sys._getframe().f_code.co_name
        appname = str(pl.node()) + ":" + path + ":" + meth
        prlog(log, msg + " error while initialising params")
        mails = ["rk_g@optum.com"]
        send_email(mails, error, msg)
        eject(time, appname, line, msg)

    try:
        query = "SELECT max(posted_date) FROM bpmda.cdmoptum_posteddates"

        df = srcconxn.sql(query).to_pandas()

        maxpd = df.iloc[0, 0]

        if maxpd is None:
            maxpd = "1900-01-01"

        print("Max Posted Date:", maxpd)

    except Exception as e:
        msg = getattr(e, 'message', str(e))
        line = currentframe().f_lineno
        now = dt.datetime.now()
        time = now.strftime("%Y-%m-%d %H:%M:%S")
        meth = sys._getframe().f_code.co_name
        appname = str(pl.node()) + ":" + path + ":" + meth
        prlog(log, "Max posted date retrieve error: " + msg)
        mails = ["rk_g@optum.com"]
        send_email(mails, error, msg)
        eject(time, appname, line, msg)


#########################################################################
# Main
#########################################################################
def start():

    sttime = dt.datetime.now()

    config()
    datasrc()

    params()

    prlog(log, "Process Started for CDMExtract job")

    stdate = sttime.strftime("%Y-%m-%d %H:%M:%S")
    meth = sys._getframe().f_code.co_name
    appname = str(pl.node()) + ":" + path + ":" + meth

    startlog = (
        stdate,
        loglevel,
        logclass,
        appname,
        currentframe().f_lineno,
        "START"
    )

    query = """
    insert into dbo.Log
    (TimeStamp, LogLevel, LogClass, AppName, LineNum, Message)
    VALUES {0}
    """.format(tuple(startlog))

    execute(logdb, query)

    querylist = queries()

    extract_and_load(querylist)

    original1 = filename

    target1 = r'\\nasv0506\optumcdmstage\PeopleSoftData\Prod\received'
    target2 = r'\\nasv0506\optumcdmstage\PeopleSoftData\Stage\received'

    shutil.copy(original1, target1)
    shutil.copy(original1, target2)

    endtime = dt.datetime.now()

    enddate = endtime.strftime("%Y-%m-%d %H:%M:%S")

    end = (
        enddate,
        loglevel,
        logclass,
        appname,
        currentframe().f_lineno,
        "FINISHED"
    )

    endquery = """
    insert into dbo.Log
    (TimeStamp, LogLevel, LogClass, AppName, LineNum, Message)
    VALUES {0}
    """.format(tuple(end))

    execute(logdb, endquery)

    msg = f"CDM Optum Peoplesoft extract is finished. {count} rows of data were produced."

    send_email(watchlist, status, msg)

    prlog(log, "Processing completed")
    prlog(log, "Total processing time " + str(endtime - sttime))

    log.close()


#########################################################################
# Start Here
#########################################################################
start()
