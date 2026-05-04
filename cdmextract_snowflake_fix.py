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
# Process extraction and loading of the data                            #
#########################################################################
def extract_and_load(querylist):
    global count
    global newmax
    try:
        for query,filename in querylist:
            extdata = srcconxn.sql(query).to_pandas()
            prlog(log,"Data extracted successfully")
    except Exception as e:
        print(e)
        msg = getattr(e, 'message', str(e))
        line = currentframe().f_lineno
        now = dt.datetime.now()
        time = now.strftime("%Y-%m-%d %H:%M:%S")
        meth = sys._getframe(  ).f_code.co_name
        appname = str(pl.node())+":"+path+":"+meth
        prlog(log,msg+" error while extracting the data at "+meth+" method")
        mails = ["rk_g@optum.com"]
        send_email(mails,error,msg)
        eject(time,appname,line,msg) 
    try:
        extdata.columns = extdata.columns.str.lower()
        extdata.columns=['unit', 'oper unit','account', 'location','dept', 'journal id','user','project','line descr','year','period', 'source','amount','posted', 'reversal_cd', 'runtime', 'descr']
        extdata.fillna("",inplace=True)
        extdata.to_csv(filename,index =None,quoting=csv.QUOTE_ALL, quotechar='"',doublequote=True)
        count=extdata.shape[0]
        newmax=extdata["posted"].max()
        print(newmax)
        msg = f"CDM Optum Peoplesoft exctract is finished. {count} rows of data were produced."
        prlog(log,msg)
        rowcount = "Rowcount: "+str(count)
        now = dt.datetime.now()
        time = now.strftime("%Y-%m-%d %H:%M:%S")
        meth = sys._getframe(  ).f_code.co_name
        appname = str(pl.node())+":"+path+":"+meth
        rc = (time,loglevel,logclass,appname,currentframe().f_lineno,rowcount)
        rowquery = """insert into Log(TimeStamp, LogLevel, LogClass, AppName,LineNum,Message) VALUES {0}""".format(tuple(rc))
        execute(logdb,rowquery)
        prlog(log,"Log table updated successfully")
        prlog(log,"Data loaded to file successfully")
    except Exception as e:
        msg = getattr(e, 'message', str(e))
        line = currentframe().f_lineno
        now = dt.datetime.now()
        time = now.strftime("%Y-%m-%d %H:%M:%S")
        meth = sys._getframe(  ).f_code.co_name
        appname = str(pl.node())+":"+path+":"+meth
        prlog(log,msg+" error while loading the data to file at "+meth+" method")
        mails = ["rk_g@optum.com"]
        send_email(mails,error,msg)
        eject(time,appname,line,msg) 
    
    #try:
    #    if count != 0:
    #        data = [{"column1": f"{drilldate}", "column2": f"{newmax}"}]
    #        df = srcconxn.create_dataframe(data)
    #        df.write.save_as_table("bpmda.cdmoptum_posteddates",mode="append")
    #    else :
    #        pass
        
    #    prlog(log,"cdmoptum_posteddates updated with new maxpd")
    #except Exception as e:
    #    msg = getattr(e, 'message', str(e))
    #    line = currentframe().f_lineno
    #    now = dt.datetime.now()
    #    time = now.strftime("%Y-%m-%d %H:%M:%S")
    #    meth = sys._getframe(  ).f_code.co_name
    #    appname = str(pl.node())+":"+path+":"+meth
    #    prlog(log,msg+" error while updating cdmoptum_posteddates table with new maxpd "+meth+" method")
    #    mails = ["rk_g@optum.com"]
    #    send_email(mails,error,msg)
    #    eject(time,appname,line,msg) 
        
#########################################################################
# Wait for completion of the up line dependencies                       #
#########################################################################

def jobupline(jobnames):
    joblist = "'" + jobnames[0] + "'"
    check = len(jobnames)
    try:
        if check > 1:
            for i in range (1 ,len(jobnames)):
                joblist = joblist + ",'" + jobnames[i] + "'"

        query = "select count(*) from bpmda.bpmjoblog where job in (" + joblist + ") and status=0"
        df = srcconxn.sql(query).to_pandas()
        
        result = df.iloc[0, 0]
        print("joblog",result)
                        
        if (result == check):
            prlog(log, "All dependencies completed")
        else:
            while True:
                df = srcconxn.sql(sqlcmd).to_pandas()
                result = df.iloc[0, 0]
                if (result == check):
                    prlog(log, "All dependencies completed")
                prlog(log,"Waiting for "+ joblist+" to finish")
                tm.sleep(60)
            
    except Exception as e:
        msg = getattr(e, 'message', str(e))
        line = currentframe().f_lineno
        now = dt.datetime.now()
        time = now.strftime("%Y-%m-%d %H:%M:%S")
        meth = sys._getframe(  ).f_code.co_name
        appname = str(pl.node())+":"+path+":"+meth
        prlog(log,msg+" error while completing the dependencies")
        mails = ["rk_g@optum.com"]
        send_email(mails,error,msg)
        eject(time,appname,line,msg) 

#########################################################################
# Set the extraction queries along with source and destination          #
# details                                                               #
#########################################################################
def queries():
    sqlquery = []
    sqlquery.append([" SELECT business_unit, operating_unit , account, Location, deptid Dept, journal_id , " + \
                    " CASE WHEN c.employeename IS NULL THEN a.oprid ELSE c.employeename END " + \
                    " , project_id Project, replace(line_descr, '\"', ' '), year(drill_date) Year, month(drill_date) " + \
                    " Period, Source, sum(Amount) Amount, posted_date, reversal_cd, current_timestamp() Runtime, descr " + \
                    " FROM optum.vw_fdw_actuals a join tleaves b on concat('S',a.operating_unit)=b.leaf and b.node='OPTUM MG' " + \
                    " left join bpmda.operator c on a.oprid=c.employeeid " + \
                    " WHERE ledger='GAAP' AND drill_date='" + str(drilldate)  + "'  AND project_id like '1%' AND posted_date <= '" +str(maxpd) + \
                    " ' GROUP BY business_unit, operating_unit, account, Location, deptid, journal_id, project_id, line_descr, " + \
                    " drill_date, Source, posted_date, reversal_cd, current_timestamp(), descr " + \
                    " , CASE WHEN c.employeename IS NULL THEN a.oprid ELSE c.employeename END" ,filename])
                                            
               
    return sqlquery

##########################################################################
# Initializing the global variables used in this module                  #
##########################################################################
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
    global maxdate
    global filename
    global maxpd
    
    path =os.getcwd().split('\\')[-2:]
    path = ".".join(path)
    path =path+".cdmextract.py"
    loglevel ="Outline"
    logclass = "DataETL"
    delim = '\t'
    status = "Status of CDMExtract job"
    error = "Error..! CDMExtract job finished with Errors"
    log = open("CDMExtract.txt","w")

    try:
        watchlist = readconfig('cdm_emails').split(',')
        last_day_of_prev_month = dt.datetime.today().replace(day=1) - dt.timedelta(days=1) 
        start_day_of_prev_month = dt.datetime.today().replace(day=1) - dt.timedelta(days=last_day_of_prev_month.day)
        #drilldate=start_day_of_prev_month.strftime("%Y-%m-%d")
        drilldate = "2026-04-01"
        
        srcconxn = cdm_opeator()
        
        file = readconfig("cdmextract")
        filepath = readconfig("path")
        filename= filepath+file+ str(dt.datetime.today().strftime("%Y-%m-%d"))+"_snowflake.csv"
        logdb = readconfig("logdb")
    except Exception as e:
        msg = getattr(e, 'message', str(e))
        line = currentframe().f_lineno
        now = dt.datetime.now()
        time = now.strftime("%Y-%m-%d %H:%M:%S")
        meth = sys._getframe(  ).f_code.co_name
        appname = str(pl.node())+":"+path+":"+meth
        prlog(log,msg+" error while initialising params at "+meth+" method")
        ####################Change mail id#####################
        mails = ["rk_g@optum.com"]
        send_email(mails,error,msg)
        eject(time,appname,line,msg) 
    try:
        query = "SELECT max(posted_date) from bpmda.cdmoptum_posteddates"
        df = srcconxn.sql(query).to_pandas()
        maxpd = df.iloc[0, 0]
        if maxpd == None:
            maxpd = "1900-01-01"
        #filename= filename+str(maxpd)+".csv"
        maxpd = "2026-04-23"    
        print(drilldate)
        print(maxpd)
    except Exception as e:
        msg = getattr(e, 'message', str(e))
        line = currentframe().f_lineno
        now = dt.datetime.now()
        time = now.strftime("%Y-%m-%d %H:%M:%S")
        meth = sys._getframe(  ).f_code.co_name
        appname = str(pl.node())+":"+path+":"+meth
        prlog(log," Max posted date retrieve error: "+msg)
        ####################Change mail id#####################
        mails = ["rk_g@optum.com"]
        send_email(mails,error,msg)
        eject(time,appname,line,msg) 

##########################################################################
# Main process block to extract and load the data                        #
##########################################################################
def start():
    sttime = dt.datetime.now()
    config()
    datasrc()
    
    params()
    prlog(log,"Process Started for CDMExtract job")
    stdate = sttime.strftime("%Y-%m-%d %H:%M:%S")
    meth = sys._getframe(  ).f_code.co_name
    appname = str(pl.node())+":"+path+":"+meth
    start = (stdate,loglevel,logclass,appname,currentframe().f_lineno,"START")
    query = """insert into dbo.Log(TimeStamp, LogLevel, LogClass, AppName,LineNum,Message) VALUES {0}""".format(tuple(start))
    execute(logdb,query)
    #jobupline(["fdw_actuals"])
    querylist = queries()
    extract_and_load(querylist)
    
    original1 = filename
    target1 = r'\\nasv0506\optumcdmstage\PeopleSoftData\Prod\received'
    target2 = r'\\nasv0506\optumcdmstage\PeopleSoftData\Stage\received'
    shutil.copy(original1, target1)
    shutil.copy(original1, target2)
    endtime = dt.datetime.now()
    
    enddate = endtime.strftime("%Y-%m-%d %H:%M:%S")
    end = (enddate,loglevel,logclass,appname,currentframe().f_lineno,"FINISHED")
    endquery = """insert into dbo.Log(TimeStamp, LogLevel, LogClass, AppName,LineNum,Message) VALUES {0}""".format(tuple(end))
    execute(logdb,endquery)
    msg = f"CDM Optum Peoplesoft exctract is finished. {count} rows of data were produced."
    send_email(watchlist,status,msg)
    prlog(log,"processing completed")
    prlog(log,"total processing time " + str(endtime - sttime))
    log.close()
    
    
##########################################################################
# Process starts here                                                    #
##########################################################################
start()
