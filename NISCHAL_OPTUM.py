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

filename =  path1+"optum_cig_gaap_actul_lnd_zone_"+ext+".csv"
filename1 = path1+"optum_cig_gaap_row_cnt_"+ext+".csv"
filename2 = path1+"optum_cig_gaap_actul_idea_nbr_lnd_zone_"+ext+".csv"
user = "cfmp_actul_load_usr"
password = password
host = 'rp000192956'
port = 3306
database = 'cfmp01'

today_date = datetime.today().date()


#defining the extract data functions from snowflake to csv

def extract():
    session=Connection()
    query1="""SELECT concat('A',account) gl_acct , ledger lgr , concat('FY',year(drill_date)) fisc_yr ,
substring('JanFebMarAprMayJunJulAugSepOctNovDec',month(drill_date)*3-2, 3) mo,concat('J',project_id) ucmg_id ,sum(Amount) Amt,
TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH:MI:SS.FF3') insrt_on,  business_unit unit, operating_unit operunit,Location,
deptid Dept,reversal_cd,CASE WHEN c.employeename IS NULL THEN a.oprid ELSE c.employeename END,journal_id,posted_date
FROM optum.vw_fdw_actuals a join tleaves b on concat('S',a.operating_unit)=b.leaf and b.node='OPTUM MG'
left join bpmda.operator c on a.oprid=c.employeeid WHERE ledger = 'GAAP' AND drill_date = '2026-04-01'
AND project_id like '1%' GROUP BY account,ledger,drill_date,project_id,insrt_on,unit,operunit,Location,Dept,reversal_cd,journal_id,posted_date
,CASE WHEN c.employeename IS NULL THEN a.oprid ELSE c.employeename END"""
    #query1="""SELECT concat('A',account) gl_acct , ledger lgr , concat('FY',year(drill_date)) fisc_yr ,substring('JanFebMarAprMayJunJulAugSepOctNovDec',month(drill_date)*3-2, 3) mo,concat('J',project_id) ucmg_id ,sum(Amount) Amt, current_timestamp() insrt_on,  business_unit unit, operating_unit operunit,Location,deptid Dept,reversal_cd FROM optum.vw_fdw_actuals a join tleaves b on concat('S',a.operating_unit)=b.leaf and b.node='OPTUM MG' left join bpmda.operator c on a.oprid=c.employeeid WHERE ledger = 'GAAP' AND drill_date = '2025-09-01'  AND project_id like '1%' GROUP BY account,ledger,drill_date,project_id,insrt_on,unit,operunit,Location,Dept,reversal_cd""" 
    #query1= """SELECT concat('A',account) gl_acct , ledger lgr , concat('FY',year(drill_date)) fisc_yr ,substring('JanFebMarAprMayJunJulAugSepOctNovDec',month(drill_date)*3-2, 3) mo,concat('J',project_id) ucmg_id ,sum(Amount) Amt, current_timestamp() insrt_on FROM optum.vw_fdw_actuals a join tleaves b on concat('S',a.operating_unit)=b.leaf and b.node='OPTUM MG' left join bpmda.operator c on a.oprid=c.employeeid WHERE ledger = 'GAAP' AND drill_date = '2025-09-01'  AND project_id like '1%' GROUP BY account,ledger,drill_date,project_id,insrt_on"""
     
    df1 = session.sql(query1).to_pandas()

    query2='select * from bpmda.cig_gaap_row_cnt'
    df2 = session.sql(query2).to_pandas()
    query3="""SELECT concat('A',account) gl_acct , ledger lgr , concat('FY',year(drill_date)) fisc_yr ,
substring('JanFebMarAprMayJunJulAugSepOctNovDec',month(drill_date)*3-2, 3) mo,concat('J',project_id) ucmg_id ,sum(Amount) Amt,
replace(line_descr, '\"', ' ') line_desc,descr,CASE WHEN c.employeename IS NULL THEN a.oprid ELSE c.employeename END,Source src,
journal_id jnl_id , TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH:MI:SS.FF3') insrt_on,business_unit unit, operating_unit operunit,Location,
deptid Dept,posted_date,reversal_cd FROM optum.vw_fdw_actuals a join tleaves b on concat('S',a.operating_unit)=b.leaf and   b.node='OPTUM MG'
left join bpmda.operator c on a.oprid=c.employeeid WHERE ledger = 'GAAP' AND drill_date = '2026-04-01'  AND project_id like '1%'
GROUP BY account,ledger,drill_date,project_id,line_descr,descr,oprid,Source,journal_id,insrt_on,unit, operunit,Location,Dept,
posted_date,reversal_cd,CASE WHEN c.employeename IS NULL THEN a.oprid ELSE c.employeename END"""

    #query3="""SELECT concat('A',account) gl_acct , ledger lgr , concat('FY',year(drill_date)) fisc_yr , substring('JanFebMarAprMayJunJulAugSepOctNovDec',month(drill_date)*3-2, 3) mo,concat('J',project_id) ucmg_id ,sum(Amount) Amt, replace(line_descr, '\"', ' ') line_desc,descr,oprid opr_id,Source src, journal_id jnl_id ,current_timestamp() insrt_on FROM optum.vw_fdw_actuals a join tleaves b on concat('S',a.operating_unit)=b.leaf and   b.node='OPTUM MG' left join bpmda.operator c on a.oprid=c.employeeid WHERE ledger = 'GAAP' AND drill_date = '2025-09-01'  AND project_id like '1%' GROUP BY account,ledger,drill_date,project_id,line_descr,descr,oprid,Source,journal_id,insrt_on """
    #query3 ="""SELECT concat('A',account) gl_acct , ledger lgr , concat('FY',year(drill_date)) fisc_yr , substring('JanFebMarAprMayJunJulAugSepOctNovDec',month(drill_date)*3-2, 3) mo,concat('J',project_id) ucmg_id ,sum(Amount) Amt, replace(line_descr, '\"', ' ') line_desc,descr,oprid opr_id,Source src, journal_id jnl_id ,current_timestamp() insrt_on,business_unit unit, operating_unit operunit,Location,deptid Dept,reversal_cd FROM optum.vw_fdw_actuals a join tleaves b on concat('S',a.operating_unit)=b.leaf and   b.node='OPTUM MG' left join bpmda.operator c on a.oprid=c.employeeid WHERE ledger = 'GAAP' AND drill_date = '2025-09-01'  AND project_id like '1%' GROUP BY account,ledger,drill_date,project_id,line_descr,descr,oprid,Source,journal_id,insrt_on,unit, operunit,Location,Dept,reversal_cd """ 


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
    df1.columns=['gl_acct', 'lgr', 'fisc_yr', 'mo', 'ucmg_id', 'amt', 'insrt_on','unit', 'oper unit','Location','Dept','reversal_cd','user','journal_id','posted_date']
    df1.fillna("",inplace=True)
    df1.to_csv(filename,index =None,quoting=csv.QUOTE_ALL, quotechar='"',doublequote=True)



def dataload2():
    print(filename2)

    df3.columns = df3.columns.str.lower()
    #df1.columns=['unit', 'ROW_CNT','account', 'location','dept', 'journal id','user','project','line descr','year','period', 'source','amount','posted', 'reversal_cd', 'runtime', 'descr']
    df3.columns=['gl_acct', 'lgr', 'fisc_yr', 'mo', 'ucmg_id', 'amt', 'line_desc', 'descr', 'user', 'src', 'jnl_id', 'insrt_on','unit', 'oper unit','Location','Dept','posted_date','reversal_cd']
    df3.fillna("",inplace=True)
    df3.to_csv(filename2,index =None,quoting=csv.QUOTE_ALL, quotechar='"',doublequote=True)

def dataload1():

    df2.columns = df2.columns.str.lower()
    #df1.columns=['unit', 'ROW_CNT','account', 'location','dept', 'journal id','user','project','line descr','year','period', 'source','amount','posted', 'reversal_cd', 'runtime', 'descr']
    df2.columns=['insert_on','row_cnt']
    df2.fillna("",inplace=True)
    df2.to_csv(filename1,index =None,quoting=csv.QUOTE_ALL, quotechar='"',doublequote=True)

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


