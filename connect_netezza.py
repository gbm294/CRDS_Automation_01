# -*- coding: utf-8 -*-
r"""
Connect to various data sources (Netezza, Clarity, or Caboodle) through python

E. Willmot - 12/15/2017
E. Willmot - 8/9/2018 - updated with comments, and to include sqlalchemy connection to Caboodle

-_-_-_-_-_-_-_-
Notes: You will first need to run the following code to set up your encrypted password:
>>>import sys
>>>sys.path.append("U:\UWHealth\EA\ShareAll\Workgroups\Advanced Analytics\netezzaConnect") # The folder where PassEncrypt.py lives

>>>PassEncrypt.make_new_pass()   # Will prompt you to enter your password

Now your password will be saved in your home directory ('H:\\') in python_creds.creds
and a key will be saved in you C drive (C:\\Users\you123) in python_key.key.

To retrieve it and use it to connect to the servers, you will use it like so:
>>>pwd = str(PassEncrypt.get_pass())
-_-_-_-_-_-_-_-

"""

#import sys
#sys.path.append(r'U:\\UWHealth\EA\ShareAll\Workgroups\Advanced Analytics\netezzaConnect')

import PassEncrypt
#import PassEncrypt_oracle

#--------------------------------------------
# Netezza
def connect_netezza(uid,database,prod_tf):
    ##############
    # PURPOSE: Connect to a Netezza database
    #
    # ASSUMPTIONS:
    # 
    # INPUTS: uid      - Enter the user ID as a string (e.g. 'KEW364')
    #         database - Enter the database as a string (e.g. 'DEPT_EA')
    #         prod_tf  - Enter True if you want to connect to PROD, otherwise type False (no quotes)
    #
    # OUTPUTS: conn - a pyodbc connection to Netezza.
    ##############

    import pyodbc
    # Right now, Netezza is not a supported dialect within sqlalchemy,
    # so we need to use pyodbc to connect.
    
    
    if prod_tf:
        conn_string = ('DRIVER={{NetezzaSQL}};SERVER=L-MAKOPROD.UWHIS.HOSP.WISC.EDU;'
        'PORT=5480;DATABASE={};UID={};PWD={};'.format(str(database.upper()),uid,str(PassEncrypt.get_pass())))
    else:
        conn_string = ('DRIVER={{NetezzaSQL}};SERVER=LAB-NETEZZA01.UWHEALTH.WISC.EDU;'
        'PORT=5480;DATABASE={};UID={};PWD={};'.format(str(database.upper()),uid,str(PassEncrypt.get_pass())))
        
    conn = pyodbc.connect(conn_string)
                              
    try:
        conn.cursor()
        #print("NETEZZA {} Connection Successful!".format(database.upper()))
    except:
        print("NETEZZA {} Connection failed".format(database.upper()))
        
        
    # To close a connection, use conn.close()
    return conn
  
#--------------------------------------------  
# Clarity
def connect_oracle(uid):
    ##############
    # PURPOSE: Connect to Clarity (PROD)
    #
    # ASSUMPTIONS:
    # 
    # INPUTS: uid      - Enter the user ID as a string (e.g. 'KEW364')
    #
    # OUTPUTS: conn - a sqlalchemy connection to Clarity.
    ##############
    import sqlalchemy as sa

    conn_string = ('oracle://{}[EXT_UWHC]:'
    '{}@epicclarity.hosp.wisc.edu/CLARITY'.format(uid,str(PassEncrypt_oracle.get_pass())))    
    
    engine = sa.create_engine(conn_string,implicit_returning=False,coerce_to_unicode=True)
    
    try:       
        engine.connect()
        #print("CLARITY Connection Successful!")
    except:
        print("CLARITY Connection failed")
        
    # To close a connection, use engine.dispose()
    return engine
    
#--------------------------------------------  
# Caboodle
def connect_cdw(uid,database,prod_tf):
    ##############
    # PURPOSE: Connect to a Netezza database
    #
    # ASSUMPTIONS:
    # 
    # INPUTS: uid      - Enter the user ID as a string (e.g. 'KEW364')
    #         database - Enter the database as a string (e.g. 'CDWRPT')
    #         prod_tf  - Enter True if you want to connect to PROD, otherwise type False (no quotes)
    #
    # OUTPUTS: conn - a pyodbc connection to Caboodle.
    ##############

    # We can connect to Caboodle with pyodbc OR sqlalchemy. 
    # Below code uses pyodbc, sqlalchemy code is commented out below.

    import pyodbc
    
    if prod_tf:
        conn_string = ('DRIVER={{ODBC Driver 13 for SQL Server}};'
        'SERVER=mex-epiccdw;DATABASE={};'
        'UID=UWHIS\{};PWD={};'
        'Trusted_Connection=yes;'.format(str(database.upper()),uid.upper(),
                                         str(PassEncrypt.get_pass())))
    else:
        conn_string = ('DRIVER={{ODBC Driver 13 for SQL Server}};'
        'SERVER=M-CDWINT00506;DATABASE={};'
        'UID=UWHIS\{};PWD={};'
        'Trusted_Connection=yes;'.format(str(database.upper()),uid.upper(),
                                         str(PassEncrypt.get_pass())))
            
    conn = pyodbc.connect(conn_string)
    
    if prod_tf:
        try:       
            conn.cursor()
            #print("CDW PROD {} Connection Successful!".format(database.upper()))
        except:
            print("CDW PROD {} Connection failed".format(database.upper()))
    else:
        try:       
            conn.cursor()
            #print("CDW QA {} Connection Successful!".format(database.upper()))
        except:
            print("CDW QA {} Connection failed".format(database.upper()))
        
        
        
    #####
    # To use sqlalchemy to connect to Caboodle, use:
    #import urllib
    #params = urllib.quote_plus(("DRIVER={{ODBC Driver 13 for SQL Server}};SERVER=mex-epiccdw;"
    #                           "DATABASE=CDWRPT;UID={};PWD={}"
    #                           ";Trusted_Connection=yes").format(uid.upper(),
    #    str(PassEncrypt.get_pass())))
    
    #conn_string = ("mssql+pyodbc:///?odbc_connect=%s" % params)
    #####    
    
    # To close a connection, use conn.close()
    return conn
    
