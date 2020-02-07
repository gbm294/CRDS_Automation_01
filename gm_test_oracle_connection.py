# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


import sys
sys.path.append('U:/UWHealth/EA/SpecialShares/DM/CRDS/AdHocQueries/Report_Automation/CRDS_Automation_01-master')

import connect_db as cn   #connect_netezza as cn
import pandas as pd




##Read sql text file ###############################################
def import_sql(inFile):
    try:
        with open(inFile,'r', encoding='utf-8') as sqlfile:
            s = sqlfile.read() 

    except:
        print("Error opening file")
        pass

    return s
####################################################################


#sql = 'Select dpt.DEPARTMENT_NM From prod_uw..DIM_DEPARTMENT dpt limit 20;'

#sql = import_sql('U:/UWHealth/EA/SpecialShares/DM/CRDS/AdHocQueries/Requests_DATA/recurring_requests/DATA_Herringa_RITM0525224/test.sql')      #'U:/UWHealth/EA/SpecialShares/DM/CRDS/AdHocQueries/Temp/python/gm_test_netezza_python.sql'
sql = '''Select
        dpt.DEPARTMENT_NAME
        From CLARITY.CLARITY_DEP dpt
        where rownum <= 100'''


print(sql)
#[ext_uwhc]
cnxn = cn.connect_oracle('GBM294')
print(cnxn)

data = pd.read_sql(sql,cnxn)

print(data['department_name'].head(5))
#print(data.head(5))

cnxn.engine.dispose()

print('\n\nDone!')


#
#
##sql = 'Select dpt.DEPARTMENT_NM From prod_uw..DIM_DEPARTMENT dpt limit 20;'
#
#sql = import_sql('U:/UWHealth/EA/SpecialShares/DM/CRDS/AdHocQueries/Temp/python/gm_test_netezza_python.sql')      
#print(sql)
#
#cnxn = cn.connect_netezza('gbm294','SDBX_DEPT_CRDS',True)
#data = pd.read_sql(sql,cnxn)
#
#print(data['DEPARTMENT_NM'].head(5))
##print(data.head(5))
#
#cnxn.close()

print('\n\nDone!')