# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.

I removed a line and am testing this with GitHub
"""


def print_gm():
    print('this is awesome')


##Import needed libraries
#Get scripts needed for recurring reports, like password encryption
import sys
sys.path.append('U:/UWHealth/EA/SpecialShares/DM/CRDS/AdHocQueries/Requests_DATA/recurring_requests/RECURRING_RESOURCES')
import time
import os
from datetime import datetime
import connect_netezza as cn  #module in RECURRING_RESOURCES
import pandas as pd           #installed library
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
    #print(df_js)

    # Exporting data records from project
#    data_exp = [
#        ('token', api_token), 
#        ('content', 'record'),
#        ('format', 'json'),
#        ('type', 'flat')
#    ]
    
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
    return r_imp
     


##Email ############################################################
def email(receivers, subject, text):
    import smtplib
    sender = 'crds@ictr.wisc.edu'
    #receivers = ['gmcmahan@wisc.edu','crds@ictr.wisc.edu']
    receiver_str = ', '.join(receivers)
    #subject = 'RITM1234000: RITM SUBJECT'
    
    message_header = """From: CRDS <crds@ictr.wisc.edu>\nTo: %s\nSubject: %s\n\n\n""" % (receiver_str, subject)
    
    message = message_header + text
    #print(message)
    
    try:
       smtpObj = smtplib.SMTP('smtp.wiscmail.wisc.edu')
       smtpObj.sendmail(sender, receivers, message)         
       print("Successfully sent email")
    except SMTPException:
       print("Error: unable to send email")
    print('Done Sending email')
####################################################################





def main():
    
    today = datetime.today().strftime('%Y_%m_%d')
    day_log_filename = 'CRDS_RECURRING_MASTER_LOG_%s.txt' % today
    #Log file for day. This should hold basic info for all CRDS recurring reports for the day.
    daily_log_file_path = 'U:/UWHealth/EA/SpecialShares/DM/CRDS/AdHocQueries/Report_Automation/LOGS/%s' % day_log_filename
    ##############################################################################################################
    today_log_exists = os.path.isfile(daily_log_file_path)  #Check if file exists.  If not, create it.
    if today_log_exists:
        pass
    else:
        with open(daily_log_file_path, 'w') as writer:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            writer.write('File created %s\n' % (now) )
    ##############################################################################################################
        
    #Get configuration file from input
    input_config = sys.argv[1]
    #Manually specify config file for testing
    #input_config = 'U:/UWHealth/EA/SpecialShares/DM/CRDS/AdHocQueries/Requests_DATA/recurring_requests/DATA_Jackson_RITM0690709/Automation/automation_scripts_config_be_careful/Jackson_RITM0690709_D2_RITM0440034_config.cfg'
    write_to_log(daily_log_file_path, input_config, 'kicked off job')  #To main log
    ##############################################################################################################
    
    
    #Pull RITM information from config file
    ritm_config_dict = setup_filenames(input_config)
    #RITM Specific logging file
    ritm_log_file_path = ritm_config_dict['automation_dir_path'] + ritm_config_dict['log_file']
    #script_name = os.path.basename(__file__)
    write_to_log(ritm_log_file_path, input_config, 'started script')  #To specific RITM Log
    ##############################################################################################################


    ## Execute Query that runs the database process and creates an output table.  This is the run all file.
    sql_execute_file_01_text = read_sql_file(ritm_config_dict['automation_dir_path'] + ritm_config_dict['sql_execute_file_01'])  #Get SQL text
    write_to_log(ritm_log_file_path, input_config, 'read_sql_file completed')  #To specific RITM Log
    exe_sql_file(sql_execute_file_01_text)  #execute SQL text.  Does not get anything back because this query just processes to Netezza.
    write_to_log(ritm_log_file_path, input_config, 'exe_sql_file completed')  #To specific RITM Log
    ##############################################################################################################
    
    
    ##Execute Query that takes data from output table and saves it as a csv file
    sql_extract_file_text = read_sql_file(ritm_config_dict['automation_dir_path'] + ritm_config_dict['sql_extract_file'])
    df_output = sql_file_to_csv(sql_extract_file_text)  #run SQL file and get output as pandas dataframe.  This step pulls the data from Netezza.
    write_to_log(ritm_log_file_path, input_config, 'sql_file_to_csv completed')
    row_count = len(df_output.index) # number of rows in output
    write_to_log(ritm_log_file_path, input_config, 'rows in todays pull - %d' %(row_count))  #To specific RITM Log
    write_to_log(daily_log_file_path, input_config, 'rows in todays pull - %d' %(row_count))  #To main log
    ##############################################################################################################


    ##If there are output rows, upload them to REDCap
    if row_count > 0:
        print('Need to upload to REDCap')
        ##need up modify headers for REDCap.  Need them in lowercase
        rc_headers_upper = list(df_output)
        rc_headers = []
        for row in rc_headers_upper:
            rc_headers.append(row.lower())
        #rc_headers = ['record_id', 'department_id', 'department_name', 'record_id']  ##Can specify headers manually to match REDCap
        df_output.columns = rc_headers
        output_to_csv(df_output, ritm_config_dict['output_file'])
        write_to_log(ritm_log_file_path, input_config, 'updated output headers')  #To specific RITM Log
        try:
            post_message = redcap_upload(ritm_config_dict) #Upload to REDCap
            write_to_log(ritm_log_file_path, input_config, post_message) #To specific RITM Log
            write_to_log(daily_log_file_path, input_config, 'Added - %d records to REDCap\n' %(row_count)) #To main log
        except:
            print('Failed upload to REDCap')
            write_to_log(ritm_log_file_path, input_config, 'Failed upload to REDCap')  #To specific RITM Log
            write_to_log(daily_log_file_path, input_config, 'Failed upload to REDCap')  #To main log
    else:
        write_to_log(daily_log_file_path, input_config, 'No records uploaded today\n')
    ##############################################################################################################

    er = False
    if 'ERROR' in post_message.upper() or 'FAILED' in post_message.upper():
        er = True

    ##Send email to Study Team
    if row_count > 0 & er is False:
        team_subject = ritm_config_dict['email_subject']
        team_message = read_sql_file(ritm_config_dict['automation_dir_path'] + ritm_config_dict['email_text'])  #Get custom email message for this request
        team_message = team_message % row_count  #Add number of records to email message. Message text file needs %s in text.
        email(ritm_config_dict['email_recipients'], team_subject ,team_message)  #Send email.
        write_to_log(ritm_log_file_path, input_config, 'email sent\n')  #To specific RITM Log


    
#    ##Send email to CRDS with log information 
#    #Read today's log mailn file
#    today_log_text = read_sql_file(daily_log_file_path)
#    #Send email
#    email(ritm_config_dict['email_recipients'], subject ,today_log_text)
#    



    
    

if __name__ == "__main__":
    main()
    print('Done!')