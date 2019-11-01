# -*- coding: utf-8 -*-
"""
Created on Mon Jun 24 12:57:06 2019

@author: gbm294
"""

def print_gm():
    print('this is awesome')


##Import needed libraries
#Get scripts needed for recurring reports, like password encryption
import sys
sys.path.append('U:\\UWHealth\\EA\\SpecialShares\\DM\\CRDS\\AdHocQueries\\Requests_DATA\\recurring_requests\\RECURRING_RESOURCES')
import time
import os
#import datetime as dt
from datetime import datetime
import connect_netezza as cn  #module in RECURRING_RESOURCES
import pandas as pd           #installed library
#import pyodbc
start = time.time()



## Set up filenames using config file #################################################
def setup_filenames(cfg_file):
    import json
    try:
        with open(cfg_file,'r', encoding='utf-8') as file:
            s = file.read()
            ritm_config_dict = json.loads(s)

            automation_dir_path         = ritm_config_dict['automation_dir_path']
            data_delivery_folder        = ritm_config_dict['data_delivery_folder']
            out_file_name_start         = ritm_config_dict['out_file_name_start']
            out_file_name_end           = ritm_config_dict['out_file_name_end']

            today = datetime.today().strftime('%Y_%m_%d')
            
            output_file = '%s%s%s%s_%s_%s%s' %(automation_dir_path,'/',data_delivery_folder,out_file_name_start,today,out_file_name_end,'.csv')
            ritm_config_dict['output_file']= output_file
            
    except:
        print("Error reading file %s" % cfg_file)
        pass

    return ritm_config_dict
    

#####################################################################
def write_to_log(logfile, script_name, action):
    with open(logfile, 'a') as writer:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        writer.write('%s -  %s -  %s\n' % (now, script_name, action) )
        

######################################################################
def read_sql_file(inFile):
    try:
        with open(inFile,'r', encoding='utf-8') as sqlfile:
            s = sqlfile.read() 
    except:
        print("Error opening file %s" % inFile)
        pass
    return s
####################################################################


##Excecute SQL file ################################################
##Connects to Netezza with ODBC connection and runs the specified SQL file
def exe_sql_file(sql_file):
    #Create connection to Netezza database
    cnxn = cn.connect_netezza('gbm294','SDBX_DEPT_CRDS',True)
    with cnxn:
        with cnxn.cursor() as crs:
            print('Running SQL Query %s \n\n' % (sql_file))
            # crs.close() will automatically be called when Python leaves the inner `with` statement
            # cnxn.commit() will automatically be called when Python leaves the outer `with` statement
            crs.execute(sql_file)
    print('\n\nDone executing SQL file!')
####################################################################

##SQL Output csv ###################################################
##Connects to Netezza with ODBC connection and pulls output data into pandas dataframe
def sql_file_to_csv(sql_file):
    print(sql_file)
    cnxn = cn.connect_netezza('gbm294','SDBX_DEPT_CRDS',True)
    with cnxn:
        # cnxn.commit() will automatically be called when Python leaves the outer `with` statement
        data = pd.read_sql(sql_file,cnxn)
        print(data.head(5))
        data = data.sort_values(by = ['RECORD_ID'])
        
    print('\n\nDone extracting SQL file!')
    return data
####################################################################


##writes data from output dataframe to a specified csv file #############
def output_to_csv(df,output_file):
    df.to_csv(output_file, index = False)
    print('done writing file')
####################################################################

##Read text file ###############################################
def read_txt_file(inFile):
    try:
        with open(inFile,'r', encoding='utf-8') as sqlfile:
            s = sqlfile.read() 
    except:
        print("Error opening file %s" % inFile)
        pass
    return s
####################################################################


###Upload to REDCap if there are Recorder to upload #################
def redcap_upload(ritm_config_dict):
    import requests
    tkn_file = ritm_config_dict['automation_dir_path'] + ritm_config_dict['api_file']
    api_token = read_txt_file(tkn_file)
    
    print(api_token)
    
    ## Read the csv output file from today's data ##
    df = pd.read_csv(ritm_config_dict['output_file'])
    print(df.head(10))
    
    ## Convert today's data upload to json
    df_js = df.to_json(orient = 'records')
    print(df_js)

    # Exporting data records from project
#    data_exp = [
#        ('token', api_token), 
#        ('content', 'record'),
#        ('format', 'json'),
#        ('type', 'flat')
#    ]
    #print('\n\n\n')
    
    # Uploading data records to project
    data_imp = [
        ('token', api_token), 
        ('content', 'record'),
        ('format', 'json'),
        ('type', 'flat'),
        
        ('overwriteBehavior','normal'),
        ('forceAutoNumber','false'),
        ('data',df_js)
    ]    
    
    #print(data_imp)
    
    print('\n\n\n')
    
    try:
        #r_ex = requests.post('https://redcap.ictr.wisc.edu/api/', data_exp).content.decode('utf-8')  #This returns records from the project
        r_imp = requests.post('https://redcap.ictr.wisc.edu/api/', data_imp).content.decode('utf-8')  #This sends records to the project
        print('')
    except:
        print('problem with upload')
        print(data_imp)
        pass
    
    #print(r_ex)
#    print('\n\n\n')
    print(r_imp)
    return 'Upload complete.'
     

####################################################################





def run_script(CONFIG_FILE):
    #need to write log file
    
    print(CONFIG_FILE)
    print('\n')
    
    #get filenames from top of file
    config_path = CONFIG_FILE
    ritm_config_dict = setup_filenames(config_path)
    #    for k, v in ritm_config_dict.items():
    #        print(k + '; ', v)
    log_file_path = ritm_config_dict['automation_dir_path'] + ritm_config_dict['log_file']
    script_name = os.path.basename(__file__)
    
    #script_name = 'THIS SCRIPT NAME'
    write_to_log(log_file_path, script_name, 'started script')
    
    ## Execute Query that runs the database process and creates an output table
    sql_execute_file_01_text = read_sql_file(ritm_config_dict['automation_dir_path'] + ritm_config_dict['sql_execute_file_01'])
    write_to_log(log_file_path, script_name, 'read_sql_file completed')
    
    
    exe_sql_file(sql_execute_file_01_text)
    write_to_log(log_file_path, script_name, 'exe_sql_file completed')
    
    ##Execute Query that takes data from output table and saves it as a csv file
    sql_extract_file_text = read_sql_file(ritm_config_dict['automation_dir_path'] + ritm_config_dict['sql_extract_file'])
    
    #run database query
    df_output = sql_file_to_csv(sql_extract_file_text)
    write_to_log(log_file_path, script_name, 'sql_file_to_csv completed')


    row_count = len(df_output.index) # number of rows in delivery
    write_to_log(log_file_path, script_name, 'rows added - %d' %(row_count))
        
    
    if row_count > 0:
        print('Uploading to REDCap')
    else:
        write_to_log(log_file_path, script_name, 'no records to upload\n\n')
        ##Need to add sending email summary
        sys.exit('no records to upload to REDCap')
        
    #replace headers with REDCap headers.  this part may need work
    rc_headers_upper = list(df_output)
    rc_headers = []
    for row in rc_headers_upper:
        rc_headers.append(row.lower())
    
    #rc_headers = ['record_id', 'DEPARTMENT_ID', 'DEPARTMENT_NAME', 'SPECIALTY']
    df_output.columns = rc_headers
    
    
    
    output_to_csv(df_output, ritm_config_dict['output_file'])


    #update log file
    write_to_log(log_file_path, script_name, 'script completed')


    #Upload to REDCap
    post_message = redcap_upload(ritm_config_dict)
    
    write_to_log(ritm_config_dict['log_file'], script_name, post_message)
    write_to_log(ritm_config_dict['log_file'], script_name, 'Added - %d records to REDCap' %(row_count))
    write_to_log(ritm_config_dict['log_file'], script_name, 'REDCap Upload complete\n\n')
    
    
    #Where we add email info to study team.







    
#Manually specify config file
#run_script('U:/UWHealth/EA/SpecialShares/DM/CRDS/AdHocQueries/Requests_DATA/recurring_requests/DATA_Burnside_RITM0481143/Automation/automation_scripts_config_be_careful/RITM0481143_BURNSIDE_RECURRING_CONFIG.cfg')