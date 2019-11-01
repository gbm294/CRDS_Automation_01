# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 16:42:54 2019

@author: gbm294
"""

import time
#import os
from datetime import datetime
import csv
start = time.time()

import recurring_parent_netezza_simple as netezza_simple



#####################################################################
def write_to_log(logfile, script_name, action):
    with open(logfile, 'a') as writer:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        writer.write('%s -  %s -  %s\n' % (now, script_name, action) )


## Set up filenames using config file #################################################
def report_list(report_list_path):
    try:
        rlist = []
        with open(report_list_path, 'r') as csvfile:
            report_dict = csv.DictReader(csvfile) #csv file to ordered dictionary
            for row in report_dict:             #Can't just pass ordered dictinary, so creating list of dictionaries to pass
                rlist.append(row)
#            #print(row['RITM'],'\n',row['PI'],'\n',row['CONFIG_FILE'],'\n\n')
#            for k, v in row.items():
#                print(k, v)
    except:
        print("Error reading file %s" % report_list_path)
        pass

    return rlist


## main #########################################################################
def main():
    #need to write log file
    try:        
        #get filenames from top of file
        report_list_path = 'U:/UWHealth/EA/SpecialShares/DM/CRDS/AdHocQueries/Requests_DATA/recurring_requests/RECURRING_RESOURCES/live_reporting_scripts/RECURRING_WEEKLY_MONDAY_MORNING.csv'
        ritm_config_dict = report_list(report_list_path)    #Get list of reports in csv file
        
        dt_format = '%Y-%m-%d'
        job_name = 'RECURRING_WEEKLY_MONDAY_MORNING'
        daily_log_file_path = 'U:/UWHealth/EA/SpecialShares/DM/CRDS/AdHocQueries/Requests_DATA/recurring_requests/RECURRING_RESOURCES/live_reporting_scripts/logs/RECURRING_WEEKLY_MONDAY_MORNING_LOG.txt'
        write_to_log(daily_log_file_path, job_name, 'kicked off job')
    except Exception as e:
        # Print any error messages to stdout
        print(e)
        write_to_log(daily_log_file_path, job_name, 'ERROR - %s' % (e))
        
    try:
        for row in ritm_config_dict:  #Loop through list of reports on list
            exp_date = datetime.strptime(row['EXPIRATION_DATE'], dt_format)
            t = datetime.today()
            
#            if exp_date < t:
#                print('Worked')
#            else:
#                print('Worked 2')
            
            
            ##Check to make sure the IRB isn't expired.
            if t < exp_date:
                cf = row['CONFIG_FILE'].replace('\\', '/')
                netezza_simple.run_script(cf)    #Calls recurring_parent_netezza_simple.py using the specified config file.
                write_to_log(daily_log_file_path, job_name, '%s sent to run' %(row['JOB_NAME']))
            else: 
                print('IRB docs expired')
                write_to_log(daily_log_file_path, job_name, 'IRB expired for %s' % (row['JOB_NAME']))
    
    
        write_to_log(daily_log_file_path, job_name, 'Done for the day  %s \n\n' % (job_name))
        
        
        
    except Exception as e:
        # Print any error messages to stdout
        print(e)
        write_to_log(daily_log_file_path, job_name, 'ERROR - %s' % (e))


if __name__ == "__main__":
    main()
    end = time.time()
    print('%d Seconds to complete' % (end - start))
    print('\nScript Complete!')