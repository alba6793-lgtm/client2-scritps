#!/usr/bin/python
########################################################################
##
##  parcon_stats_v1.7.py
##  author  : Cristian A.
##
##  Version 1.0
##  This script will generate postgresql tables for the statistics tool software.
##  This script will search for a specific patern on every CDR, in a given period of time,
##  to generate and input(postgresql tables) for the statistics tool software.
##
##  Version 1.1
##  Added the possibility to search for CDRs in the redundancies of each server.
##  Added logs for each CDR file found or not found.
##  Optimization of date calculation.
##
##  Version 1.2
##  Added functions get_cdrs(), cdr_names_list(), create_directory(), time_period_check() and
##  delete_directory() to optimize the script.
##  Now insert_statistics_x() functions receive the length of the list and not the entire list.
##  Now in the case of not having information of a specific time, the entry will be filled in with zeros.
##  
##  Version 1.3
##  Modified type of variable for "date" column in postgresql tables, now is a timestamp variable.
##  Added a new function called csv_from_psql() to generate csv files from the information added in tables.
##  Added a while loop to allow a better scp connection in cases where the time period exceeds 6 months. 
##
##  Version 1.4
##  Added a while loop to generate a CSV file per day.
##  Modified the function csv_from_psql() to get csv files with format %Y-%m-%d_%H_00_00_tablename.csv
##  Added the possibility to create the CSV directory before generating CSV files.
##  Modified the log file name, now the name starts with the script execution time
##  %Y-%m-%d_%H_%M_%S_parcon_stats_script.log, a new log file will be generated at each execution.
##
##  Version 1.5
##  Modified the function csv_from_psql() to get csv files with format %Y-%m-%d_tablename.csv
##  Added a syslog entries for start and end of the application. 
##  Changed the path where the logs are located var/local/dasp/logs/
##
##  Version 1.6
##  Modified the table abon_desab_via_ussd to have visibility of all types of errors
##  Added columns : ab_err_par_is_ch, ab_err_lang, ab_err_ch_is_par, ab_err_offer, ab_err_bad_req,
##  ab_err_bad_pass, ab_err_db, dab_err, dab_err_bad_req, dab_err_bad_pass, dab_err_db
##  Change date input format to YYYY-MM-DD
##
##  Version 1.7
##  Now the script upload CSV files from DPIs 1 and 2 to the database
##  New functions csv_to_pgsql(), get_csv() and edit_csv()
##  Function edit_csv was added because source csv file does not match pgsql requirements.
##
##  How to run the script
##  1. Without parameters it will search CDRs from yesterday. 
##     Example: today: 05/05/2021 The script will search CDRs from yesterday (04/05/2021).
##     $ python /path/parcon_stats_v1.7.py
##
##  2. With one date (10 04 2021 ), it will search CDRs from the given date to yesterday.
##     Example: today: 05/05/2021, date: 10/04/2021 The script will search CDRs from 10/04/2021 to
##     yesterday(04/05/2021).
##     $ python /path/parcon_stats_v1.7.py 2021-04-10
##
##  2. With two dates (10 04 2021 08 05 2021), it will search CDRs from the period of time.
##     Example: date1: 10/04/2021  date2: 08/05/2021 the script will search CDRs from 10/04/2021 to 08/05/2021.
##     $ python /path/parcon_stats_v1.7.py 2021-04-10 2021-05-08
##
##
##  ALTER TABLE abon_desab_via_ussd ADD COLUMN ab_err_par_is_ch INTEGER;
##  ALTER TABLE abon_desab_via_ussd ADD COLUMN ab_err_lang INTEGER;
##  ALTER TABLE abon_desab_via_ussd ADD COLUMN ab_err_ch_is_par INTEGER;
##  ALTER TABLE abon_desab_via_ussd ADD COLUMN ab_err_offer INTEGER;
##  ALTER TABLE abon_desab_via_ussd ADD COLUMN ab_err_bad_req INTEGER;
##  ALTER TABLE abon_desab_via_ussd ADD COLUMN ab_err_bad_pass INTEGER;
##  ALTER TABLE abon_desab_via_ussd ADD COLUMN ab_err_db INTEGER;
##  ALTER TABLE abon_desab_via_ussd ADD COLUMN dab_err INTEGER;
##  ALTER TABLE abon_desab_via_ussd ADD COLUMN dab_err_bad_req INTEGER;
##  ALTER TABLE abon_desab_via_ussd ADD COLUMN dab_err_bad_pass INTEGER;
##  ALTER TABLE abon_desab_via_ussd ADD COLUMN dab_err_db INTEGER;

import os
import shutil
import sys
import sys
import glob
from datetime import datetime, timedelta
import ntpath
import datetime
import time
import threading

import pexpect

import logging
import logging.handlers

import psycopg2
import psycopg2.extensions
import psycopg2.pool
from psycopg2.pool import PoolError
from collections import OrderedDict

my_logger = logging.getLogger('parcon_stats_v1.7.py')
my_logger.setLevel(logging.INFO)
handler = logging.handlers.SysLogHandler(address = '/dev/log')
my_logger.addHandler(handler)

LOG_FILENAME = '/var/local/dasp/'+datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S_parcon_stats_script.log')
logger = logging.getLogger('parcon_stats_v1.7.py')
hdlr = logging.handlers.RotatingFileHandler(LOG_FILENAME,maxBytes=52428800,backupCount=3)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)

# Maximum number of cdrs downloaded simultaneously
MAX_LEN = 1440

# For test PROD=0, on client enviroment PROD=1 
PROD = 1

if (PROD) :
# Add production tickets path
    PATH_SMS_NOTIF_1 = '/var/local/dasp/parcon/notifier/tickets/'
    PATH_SMS_NOTIF_2 = '/var/local/dasp/parcon/notifier/tickets/'
    PATH_RADIUS_REQ_1 = '/var/local/dasp/radius/tickets/'
    PATH_RADIUS_REQ_2 = '/var/local/dasp/radius/tickets/'
    PATH_ABO_USSD_1 = '/var/local/dasp/parcon/soap/subscription/tickets/'
    PATH_ABO_USSD_2 = '/var/local/dasp/parcon/soap/subscription/tickets/'
    PATH_ABO_CC_1 = '/var/local/dasp/parcon/soap/plmn/order_client/tickets/'
    PATH_ABO_CC_2 = '/var/local/dasp/parcon/soap/plmn/order_client/tickets/'
    PATH_CSV_1 = '/var/local/dasp/parcon/stats/results/'
    PATH_CSV_2 = '/var/local/dasp/parcon/stats/results/'
    
    HOST_SMS_NOTIF_1 = '192.168.104.196'    #vm-ctrl1
    HOST_SMS_NOTIF_2 = '192.168.104.197'    #vm-ctrl2
    HOST_RADIUS_REQ_1 = '192.168.177.3'     #dpi1
    HOST_RADIUS_REQ_2 = '192.168.177.5'     #dpi2
    HOST_ABO_USSD_1 = '192.168.104.196'     #vm-ctrl1
    HOST_ABO_USSD_2 = '192.168.104.197'     #vm-ctrl2
    HOST_ABO_CC_1 = '192.168.104.196'       #vm-ctrl1
    HOST_ABO_CC_2 = '192.168.104.197'       #vm-ctrl2
    HOST_CSV_1 = '192.168.177.3'            #dpi1
    HOST_CSV_2 = '192.168.177.5'            #dpi2
    USER_1 = 'dasp'
    PASSWORD_1 = 'dasp'
    USER_2 = 'dasp'
    PASSWORD_2 = 'dasp'
    DB_HOST = '127.0.0.1'
    DB_NAME = 'stats-ota'
    DB_USER = 'dasp'
    DB_PASS = 'dasp'

else :
# Add test tickets path
    PATH_SMS_NOTIF_1 = '/tmp/test/cdr-notifier/cdr-notifier/'
    PATH_SMS_NOTIF_2 = '/tmp/test/cdr-notifier/cdr-notifier/'
    PATH_RADIUS_REQ_1 = '/tmp/test/cdr-radius/cdr-radius/'
    PATH_RADIUS_REQ_2 = '/tmp/test/cdr-radius/cdr-radius/'
    PATH_ABO_USSD_1 = '/tmp/test/cdr-soap-sunscription/cdr-soap-sunscription/'
    PATH_ABO_USSD_2 = '/tmp/test/cdr-soap-sunscription/cdr-soap-sunscription/'
    PATH_ABO_CC_1 = '/tmp/test/cdr-cc-subscription/'
    PATH_ABO_CC_2 = '/tmp/test/cdr-cc-subscription/'
    PATH_CSV_1 = '/tmp/results/'
    PATH_CSV_2 = '/tmp/results/'
    
    HOST_SMS_NOTIF_1 = '192.168.1.47'
    HOST_SMS_NOTIF_2 = '192.168.1.104'
    HOST_RADIUS_REQ_1 = '192.168.1.47'
    HOST_RADIUS_REQ_2 = '192.168.1.104'
    HOST_ABO_USSD_1 = '192.168.1.47'
    HOST_ABO_USSD_2 = '192.168.1.104'
    HOST_ABO_CC_1 = '192.168.1.47'
    HOST_ABO_CC_2 = '192.168.1.104'
    HOST_CSV_1 = '192.168.1.47'
    HOST_CSV_2 = '192.168.1.104'
    USER_1 = 'zabbix'
    PASSWORD_1 = 'zabbix'
    USER_2 = 'dasp'
    PASSWORD_2 = 'dasp'
    DB_HOST = '127.0.0.1'
    DB_NAME = 'statistics'
    DB_USER = 'dasp'
    DB_PASS = 'dasp'
    
TEMP_SMS_NOTIF = '/tmp/cdr-notifier/'
TEMP_RADIUS_REQ = '/tmp/cdr-radius/'
TEMP_ABO_USSD = '/tmp/cdr-soap-subscription/'
TEMP_ABO_CC = '/tmp/cdr-cc-subscription/'
TEMP_CSV = '/var/local/dasp/parcon/stats/results/'

CSV_PATH = '/var/local/dasp/stats/'
LOG_PATH = '/var/local/dasp/logs/'

## Configuration of PostgreSQL connection
conn_string = 'dbname='+DB_NAME+' user='+DB_USER+' host='+DB_HOST+' password='+DB_PASS

## Creation of CSV files
def csv_from_psql(table_name,initial_date,final_date):
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    sql = """COPY (SELECT * FROM """+table_name+""" WHERE date between '"""+initial_date+"""' and '"""+final_date+"""') TO STDOUT WITH CSV HEADER DELIMITER ';'"""
    try:
        with open(CSV_PATH+initial_date[:10]+"_"+table_name+".csv", "w") as file:
            cur.copy_expert(sql, file)
        logger.debug('Generated CSV file from '+table_name+' date '+initial_date)
    except psycopg2.Error as e:
        logger.debug('Not possible to generate the CSV file from '+table_name+' date '+initial_date+' Error message: '+str(e.message))
        
    cur.close()
    conn.commit()
    conn.close()

## Creation of tables    
def table_creation():

    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    cur.execute("select * from information_schema.tables where table_name=%s", ('sms_notifications',))
    if cur.rowcount == 0:
        sql ='''CREATE TABLE sms_notifications (date timestamp PRIMARY KEY, total_sms_notif_envoi INT NOT NULL, sms_notif_envoi_succes INT NOT NULL, sms_notif_envoi_echec INT NOT NULL)'''
        cur.execute(sql)
        logger.debug('sms_notifications table created')
    cur.execute("select * from information_schema.tables where table_name=%s", ('requetes_radius',))
    if cur.rowcount == 0:
        sql ='''CREATE TABLE requetes_radius (date timestamp PRIMARY KEY, total_req_radius_recu INT NOT NULL, req_radius_succes INT NOT NULL, req_radius_echec INT NOT NULL)'''
        cur.execute(sql)
        logger.debug('requetes_radius table created')
    cur.execute("select * from information_schema.tables where table_name=%s", ('abon_desab_via_ussd',))
    if cur.rowcount == 0:
        sql ='''CREATE TABLE abon_desab_via_ussd (date timestamp PRIMARY KEY, total_abon_via_ussd INT NOT NULL, abon_via_ussd_succes INT NOT NULL, abon_via_ussd_echec INT NOT NULL, total_desab_via_ussd INT NOT NULL, desab_via_ussd_succes INT NOT NULL, desab_via_ussd_echec INT NOT NULL, ab_err_par_is_ch INT NOT NULL, ab_err_lang INT NOT NULL, ab_err_ch_is_par INT NOT NULL, ab_err_offer INT NOT NULL, ab_err_bad_req INT NOT NULL, ab_err_bad_pass INT NOT NULL, ab_err_db INT NOT NULL, dab_err INT NOT NULL, dab_err_bad_req INT NOT NULL, dab_err_bad_pass INT NOT NULL, dab_err_db INT NOT NULL)'''
        cur.execute(sql)
        logger.debug('abon_desab_via_ussd table created')
    cur.execute("select * from information_schema.tables where table_name=%s", ('abon_desab_via_cc',))
    if cur.rowcount == 0:
        sql ='''CREATE TABLE abon_desab_via_cc (date timestamp PRIMARY KEY, total_abon_via_cc INT NOT NULL, abon_via_cc_succes INT NOT NULL, abon_via_cc_echec INT NOT NULL, total_desab_via_cc INT NOT NULL, desab_via_cc_succes INT NOT NULL, desab_via_cc_echec INT NOT NULL)'''
        cur.execute(sql)
        logger.debug('abon_desab_via_cc table created')
    cur.close()
    conn.commit()
    conn.close()
    
## Connection to DB
def dbInsertOne(SQL,data):

    conn = None
    try:

        # start = time.time()
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        cur.execute(SQL, data)
        conn.commit()
        cur.close()
        # end = time.time()
        # logger.debug('DB,O,{},1'.format(str(end - start)))

    except (Exception, psycopg2.Error) as error:
        logger.debug('DB,O,E,0,'+str(error.message))

    finally:
        if conn is not None:
            conn.close()
            
## Generation of the psql query and insertion of the data in the corresponding table

def insert_statistics_sms_notif_envoi(date_hour,total_sms_notif_envoi,sms_notif_envoi_succes,sms_notif_envoi_echec):

    userQuery = """insert into public.sms_notifications (date,total_sms_notif_envoi,sms_notif_envoi_succes,sms_notif_envoi_echec) values (%s,%s,%s,%s)"""
    record_to_insert = (date_hour,total_sms_notif_envoi,sms_notif_envoi_succes,sms_notif_envoi_echec)
    dbInsertOne(userQuery,record_to_insert)
    
def insert_statistics_req_radius_recu(date_hour,total_req_radius_recu,req_radius_succes,req_radius_echec):
     
    userQuery = """insert into public.requetes_radius (date,total_req_radius_recu,req_radius_succes,req_radius_echec) values (%s,%s,%s,%s)"""
    record_to_insert = (date_hour,total_req_radius_recu,req_radius_succes,req_radius_echec)
    dbInsertOne(userQuery,record_to_insert)
    
def insert_statistics_abon_via_ussd(date_hour,total_abon_via_ussd,abon_via_ussd_succes,abon_via_ussd_echec,total_desab_via_ussd,desab_via_ussd_succes,desab_via_ussd_echec,ab_err_par_is_ch,ab_err_lang,ab_err_ch_is_par,ab_err_offer,ab_err_bad_req,ab_err_bad_pass,ab_err_db,dab_err,dab_err_bad_req,dab_err_bad_pass,dab_err_db):

    userQuery = """insert into public.abon_desab_via_ussd (date,total_abon_via_ussd,abon_via_ussd_succes,abon_via_ussd_echec,total_desab_via_ussd,desab_via_ussd_succes,desab_via_ussd_echec,ab_err_par_is_ch,ab_err_lang,ab_err_ch_is_par,ab_err_offer,ab_err_bad_req,ab_err_bad_pass,ab_err_db,dab_err,dab_err_bad_req,dab_err_bad_pass,dab_err_db) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    record_to_insert = (date_hour,total_abon_via_ussd,abon_via_ussd_succes,abon_via_ussd_echec,total_desab_via_ussd,desab_via_ussd_succes,desab_via_ussd_echec,ab_err_par_is_ch,ab_err_lang,ab_err_ch_is_par,ab_err_offer,ab_err_bad_req,ab_err_bad_pass,ab_err_db,dab_err,dab_err_bad_req,dab_err_bad_pass,dab_err_db)
    dbInsertOne(userQuery,record_to_insert)
                
def insert_statistics_abon_via_cc(date_hour,total_abon_via_cc,abon_via_cc_succes,abon_via_cc_echec,total_desab_via_cc,desab_via_cc_succes,desab_via_cc_echec):

    userQuery = """insert into public.abon_desab_via_cc (date,total_abon_via_cc,abon_via_cc_succes,abon_via_cc_echec,total_desab_via_cc,desab_via_cc_succes,desab_via_cc_echec) values (%s,%s,%s,%s,%s,%s,%s)"""
    record_to_insert = (date_hour,total_abon_via_cc,abon_via_cc_succes,abon_via_cc_echec,total_desab_via_cc,desab_via_cc_succes,desab_via_cc_echec)
    dbInsertOne(userQuery,record_to_insert)

## Checking paterns in each line of the CDR file and adding it to the corresponding list
def check_line_sms_notif_envoi(line) :

    total_sms_notif_envoi.append(line.split(" ")[0])
    if ',-1,' in line:
        sms_notif_envoi_echec.append(line.split(" ")[0])
    elif ',-2,' in line:
        sms_notif_envoi_echec.append(line.split(" ")[0])
    else :
        sms_notif_envoi_succes.append(line.split(" ")[0])

def check_line_req_radius_recu(line) :

    if '_A,' in line :
        total_req_radius_recu.append(line.split(" ")[0])
        if ',0' in line:
            req_radius_succes.append(line.split(" ")[0])
        else :
            req_radius_echec.append(line.split(" ")[0])
    elif '_I,' in line :
        total_req_radius_recu.append(line.split(" ")[0])
        if ',0' in line:
            req_radius_succes.append(line.split(" ")[0])
        else :
            req_radius_echec.append(line.split(" ")[0])

def check_line_abon_via_ussd(line) :

    if ' S:' in line:
        total_abon_via_ussd.append(line.split(" ")[0])
        if ' 0:0' in line:
            abon_via_ussd_succes.append(line.split(" ")[0])
        else:
            abon_via_ussd_echec.append(line.split(" ")[0])
            if ' 0:1' in line:
                ab_err_par_is_ch.append(line.split(" ")[0])
            elif ' 0:3' in line:
                ab_err_lang.append(line.split(" ")[0])
            elif ' 0:4' in line:
                ab_err_ch_is_par.append(line.split(" ")[0])
            elif ' 0:6' in line:
                ab_err_offer.append(line.split(" ")[0])
            elif ' 1\n' in line:
                ab_err_bad_req.append(line.split(" ")[0])
            elif ' 2\n' in line:
                ab_err_bad_pass.append(line.split(" ")[0])
            elif ' 3\n' in line:
                ab_err_db.append(line.split(" ")[0])
            
    elif ' U:' in line:
        total_desab_via_ussd.append(line.split(" ")[0])
        if ' 0:0' in line:
            desab_via_ussd_succes.append(line.split(" ")[0])
        else:
            desab_via_ussd_echec.append(line.split(" ")[0])
            if ' 0:1' in line:
                dab_err.append(line.split(" ")[0])
            elif ' 1\n' in line:
                dab_err_bad_req.append(line.split(" ")[0])
            elif ' 2\n' in line:
                dab_err_bad_pass.append(line.split(" ")[0])
            elif ' 3\n' in line:
                dab_err_db.append(line.split(" ")[0])

def check_line_abon_via_cc(line) :

    if '=A(' in line:
        total_abon_via_cc.append(line.split(" ")[0])
        if ' 0:0' in line:
            abon_via_cc_succes.append(line.split(" ")[0])
        else:
            abon_via_cc_echec.append(line.split(" ")[0])
    elif '=D(' in line:
        total_desab_via_cc.append(line.split(" ")[0])
        if ' 0:0' in line:
            desab_via_cc_succes.append(line.split(" ")[0])
        else:
            desab_via_cc_echec.append(line.split(" ")[0])
 
def read_files(cdr_names_2) :
    for i in range(0, len(cdr_names_2)) :
        ##  Reinitialization of list
        total_sms_notif_envoi[:] = []
        sms_notif_envoi_succes[:] = []
        sms_notif_envoi_echec[:] = []
        
        total_req_radius_recu[:] = []
        req_radius_succes[:] = []
        req_radius_echec[:] = []
        
        total_abon_via_ussd[:] = []
        abon_via_ussd_succes[:] = []
        abon_via_ussd_echec[:] = []
        ab_err_par_is_ch[:] = []
        ab_err_lang[:] = []
        ab_err_ch_is_par[:] = []
        ab_err_offer[:] = []
        ab_err_bad_req[:] = []
        ab_err_bad_pass[:] = []
        ab_err_db[:] = []
        total_desab_via_ussd[:] = []
        desab_via_ussd_succes[:] = []
        desab_via_ussd_echec[:] = []
        dab_err[:] = []
        dab_err_bad_req[:] = []
        dab_err_bad_pass[:] = []
        dab_err_db[:] = []
        
        total_abon_via_cc[:] = []
        abon_via_cc_succes[:] = []
        abon_via_cc_echec[:] = []
        total_desab_via_cc[:] = []
        desab_via_cc_succes[:] = []
        desab_via_cc_echec[:] = []
        
        
        start_counting_last_hour(cdr_names_2[i],1)
        start_counting_last_hour(cdr_names_2[i],2)
        start_counting_last_hour(cdr_names_2[i],3)
        start_counting_last_hour(cdr_names_2[i],4)
        
        
## Reading each line of CDR file    
def start_counting_last_hour(file_name,case) : 
    if case == 1:
        try: 
            f = open(TEMP_SMS_NOTIF+file_name+'.cdr',mode='r')
            for line in f:
                    check_line_sms_notif_envoi(line)                
            f.close()
            ## logger.debug('SMS_NOTIF CDR '+file_name+' found')
            insert_statistics_sms_notif_envoi(file_name,len(total_sms_notif_envoi),len(sms_notif_envoi_succes),len(sms_notif_envoi_echec))
        except IOError:
            insert_statistics_sms_notif_envoi(file_name,0,0,0)
            logger.debug('SMS_NOTIF CDR '+file_name+' not found')
            
    elif case == 2:
        try: 
            f = open(TEMP_RADIUS_REQ+file_name+'.cdr',mode='r')
            for line in f:
                    check_line_req_radius_recu(line)                
            f.close()
            ## logger.debug('RADIUS_REQ CDR '+file_name+' found')
            insert_statistics_req_radius_recu(file_name,len(total_req_radius_recu),len(req_radius_succes),len(req_radius_echec))
        except IOError:
            insert_statistics_req_radius_recu(file_name,0,0,0)
            logger.debug('RADIUS_REQ CDR '+file_name+' not found')
    elif case == 3:
        try: 
            f = open(TEMP_ABO_USSD+file_name+'.cdr',mode='r')
            for line in f:
                    check_line_abon_via_ussd(line)                
            f.close()
            ## logger.debug('ABO_USSD CDR '+file_name+' found')
            insert_statistics_abon_via_ussd(file_name,len(total_abon_via_ussd),len(abon_via_ussd_succes),len(abon_via_ussd_echec),len(total_desab_via_ussd),len(desab_via_ussd_succes),len(desab_via_ussd_echec),len(ab_err_par_is_ch),len(ab_err_lang),len(ab_err_ch_is_par),len(ab_err_offer),len(ab_err_bad_req),len(ab_err_bad_pass),len(ab_err_db),len(dab_err),len(dab_err_bad_req),len(dab_err_bad_pass),len(dab_err_db))
        except IOError:
            insert_statistics_abon_via_ussd(file_name,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)
            logger.debug('ABO_USSD CDR '+file_name+' not found')

    elif case == 4:
        try: 
            f = open(TEMP_ABO_CC+file_name+'.cdr',mode='r')
            for line in f:
                    check_line_abon_via_cc(line)                
            f.close()
            ## logger.debug('ABO_CC CDR '+file_name+' found')
            insert_statistics_abon_via_cc(file_name,len(total_abon_via_cc),len(abon_via_cc_succes),len(abon_via_cc_echec),len(total_desab_via_cc),len(desab_via_cc_succes),len(desab_via_cc_echec))
        except IOError:
            insert_statistics_abon_via_cc(file_name,0,0,0,0,0,0)
            logger.debug('ABO_CC CDR '+file_name+' not found')

## SCP connection to download CDRs
def get_cdrs(user,host_name,host_path,cdr_names,temp_path,password):
    try:
        host_connection = pexpect.spawn('scp '+user+'@'+host_name+':'+host_path+'\{'+cdr_names+'}.cdr '+temp_path)
        r=host_connection.expect ('assword:', timeout=10)
        if r==0:
            host_connection.sendline (password)
            host_connection.expect(["]#","]$", pexpect.EOF, pexpect.TIMEOUT])
        host_connection.close()
        logger.debug('Downloading CDRs from '+host_name)
    except Exception as e:
        logger.debug('Not possible to connect to '+host_name+' '+str(e))

## SCP connection to download CSV files from DPIs
def get_csv(user,host_name,host_path,csv_names,temp_path,password):
    if len(csv_names)>16:
        try:
            host_connection = pexpect.spawn('scp '+user+'@'+host_name+':'+host_path+'\{'+csv_names+'}.csv '+temp_path)
            #host_connection.logfile = sys.stdout
            r=host_connection.expect ('assword:', timeout=10)
            if r==0:
                host_connection.sendline (password)
                host_connection.expect(["]#","]$", pexpect.EOF, pexpect.TIMEOUT])
            host_connection.close()
            logger.debug('Downloading CSV files from '+host_name)
        except Exception as e:
            logger.debug('Not possible to connect to '+host_name+' '+str(e))
    else:
        try:
            host_connection = pexpect.spawn('scp '+user+'@'+host_name+':'+host_path+csv_names+'.csv '+temp_path)
            #host_connection.logfile = sys.stdout
            r=host_connection.expect ('assword:', timeout=10)
            if r==0:
                host_connection.sendline (password)
                host_connection.expect(["]#","]$", pexpect.EOF, pexpect.TIMEOUT])
            host_connection.close()
            logger.debug('Downloading CSV files from '+host_name)
        except Exception as e:
            logger.debug('Not possible to connect to '+host_name+' '+str(e))
        
def daterange(start_date, end_date):
    delta = timedelta(hours=1)
    while start_date <= end_date:
        yield start_date
        start_date += delta

## Generation of the CDR name list (hours)
def cdr_names_list(start_date, end_date):
    for single_date in daterange(start_date, end_date):
        cdr_names_2.append(single_date.strftime("%Y-%m-%d_%H:00:00"))
        
## Generation of the CSV name list (days)
def csv_names_list2_days(start_date, end_date):
    csv_names_tmp=[]
    for single_date in daterange(start_date, end_date):
        csv_names_tmp.append(single_date.strftime("%Y-%m-%d_00:00"))
    csv_names_days=list(OrderedDict.fromkeys(csv_names_tmp))
    return csv_names_days

# Edit CSV file to replace hour for a timestamp at the beginning of each line (i.e.: '02'->'2022-11-05_02')

def edit_csv(file_name):
    for csv_name_date in file_name :
        try: 
            f = open(TEMP_CSV+csv_name_date+'.csv',mode='r')
            content = ''
            for line in f:
                content=content+csv_name_date[:11]+line[:2]+':00:00'+line[2:-2]+'\n'
            f.close()

            f = open(TEMP_CSV+csv_name_date+'.csv',mode='w')
            f.write(content)
            f.close()

        except IOError:
            logger.debug('CSV file '+csv_name_date+'.csv not found')

def csv_to_psql(file_name):
    for csv_name_date in file_name :
        try:
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()
        except psycopg2.Error as e:
            logger.debug('Not possible to upload CSV file '+csv_name_date+'.csv Error message: '+str(e.message))
        try:
            f = open(TEMP_CSV+csv_name_date+'.csv','r')
            cur.copy_from(f,'general',sep=';')
            f.close()
            logger.debug('Upload CSV file '+csv_name_date+'.csv to general table')
        except IOError:
            logger.debug('CSV file '+csv_name_date+'.csv not found')    
        cur.close()
        conn.commit()
        conn.close()

## Checking input arguments 
def time_period_check(arguments):
    now = datetime.datetime.now()
    if len(arguments) > 1:
        if len(arguments) == 2:
            start_date = datetime.datetime.strptime(arguments[1],'%Y-%m-%d')
            if start_date <= now:
                end_date = datetime.datetime( now.year, now.month, now.day, 23) - datetime.timedelta (days = 1)
                cdr_names_list(start_date, end_date)
                csv_names_days=csv_names_list2_days(start_date, end_date)
                    
        elif len(arguments) > 2:
            start_date = datetime.datetime.strptime(arguments[1],'%Y-%m-%d')
            end_date = datetime.datetime.strptime(arguments[2],'%Y-%m-%d')+ datetime.timedelta (hours = 23)
            if start_date <= end_date:
                cdr_names_list(start_date, end_date)
                csv_names_days=csv_names_list2_days(start_date, end_date)
                
            else :
                cdr_names_list(end_date, start_date)
                csv_names_days=csv_names_list2_days(start_date, end_date)
                
    else:
        start_date = datetime.datetime( now.year, now.month, now.day, 0) - datetime.timedelta (days = 1)
        end_date = datetime.datetime( now.year, now.month, now.day, 23) - datetime.timedelta (days = 1)
        cdr_names_list(start_date, end_date)
        csv_names_days=csv_names_list2_days(start_date, end_date)
        
    return csv_names_days

def move_file():   
    files = glob.iglob('/var/local/dasp/*.log')
    for file in files:
        if os.path.isfile(file):
            shutil.move(file,LOG_PATH)

def create_directory(temp_path):
    try:
        os.makedirs(temp_path)
    except OSError:
        logger.debug('Not possible to create the directory, '+temp_path+' already exists.')

def delete_directory(temp_path):
    try:
        shutil.rmtree(temp_path)
    except OSError:
        logger.debug('Not possible to delete the directory '+temp_path)
        
##  Main function
if __name__ == '__main__' :
    my_logger.info('Starting parcon_stats_v1.7')
    start = time.time()
    
    ## Creation of tables if it is necessary 
    table_creation()
    
    ## Creation of the CSV directory if it is necessary
    create_directory(CSV_PATH)
    create_directory(LOG_PATH)
    
    ## CDR name list initialization
    ## cdr_names_2 = '2021-04-09_00:00:00' 
    cdr_names_2 = []
    
    ## CVS name list initialization
    ## csv_names_days = '2021-04-09_00:00:00' 
    csv_names_days = []
    
    ##  Initialization of list
    total_sms_notif_envoi = []
    sms_notif_envoi_succes = []
    sms_notif_envoi_echec = []
        
    total_req_radius_recu = []
    req_radius_succes = []
    req_radius_echec = []
        
    total_abon_via_ussd = []
    abon_via_ussd_succes = []
    abon_via_ussd_echec = []
    ab_err_par_is_ch = []
    ab_err_lang = []
    ab_err_ch_is_par = []
    ab_err_offer = []
    ab_err_bad_req = []
    ab_err_bad_pass = []
    ab_err_db = []
    total_desab_via_ussd = []
    desab_via_ussd_succes = []
    desab_via_ussd_echec = []
    dab_err = []
    dab_err_bad_req = []
    dab_err_bad_pass = []
    dab_err_db = []
        
    total_abon_via_cc = []
    abon_via_cc_succes = []
    abon_via_cc_echec = []
    total_desab_via_cc = []
    desab_via_cc_succes = []
    desab_via_cc_echec = []
    
    ## Creation of temporary directories
    create_directory(TEMP_SMS_NOTIF)
    create_directory(TEMP_RADIUS_REQ)
    create_directory(TEMP_ABO_USSD)
    create_directory(TEMP_ABO_CC)
    
    ## Checking dates and CDR File names generation
    csv_names_days=time_period_check(sys.argv)

    ## Give format to the CDR name list for the SCP connection
    cdr_names_tmp=cdr_names_2

    while len(cdr_names_tmp)>0:
        if len(cdr_names_tmp)>MAX_LEN:
            cdr_names_2_str=','.join(cdr_names_tmp[:MAX_LEN])
            cdr_names_tmp=cdr_names_tmp[MAX_LEN:]
        else :
            cdr_names_2_str=','.join(cdr_names_tmp)
            cdr_names_tmp=[]
        ## Import CDR files to a temporary directory
        get_cdrs(USER_1,HOST_SMS_NOTIF_1,PATH_SMS_NOTIF_1,cdr_names_2_str,TEMP_SMS_NOTIF,PASSWORD_1)
        get_cdrs(USER_2,HOST_SMS_NOTIF_2,PATH_SMS_NOTIF_2,cdr_names_2_str,TEMP_SMS_NOTIF,PASSWORD_2)
        get_cdrs(USER_1,HOST_RADIUS_REQ_1,PATH_RADIUS_REQ_1,cdr_names_2_str,TEMP_RADIUS_REQ,PASSWORD_1)
        get_cdrs(USER_2,HOST_RADIUS_REQ_2,PATH_RADIUS_REQ_2,cdr_names_2_str,TEMP_RADIUS_REQ,PASSWORD_2)
        get_cdrs(USER_1,HOST_ABO_USSD_1,PATH_ABO_USSD_1,cdr_names_2_str,TEMP_ABO_USSD,PASSWORD_1)
        get_cdrs(USER_2,HOST_ABO_USSD_2,PATH_ABO_USSD_2,cdr_names_2_str,TEMP_ABO_USSD,PASSWORD_2)
        get_cdrs(USER_1,HOST_ABO_CC_1,PATH_ABO_CC_1,cdr_names_2_str,TEMP_ABO_CC,PASSWORD_1)
        get_cdrs(USER_2,HOST_ABO_CC_2,PATH_ABO_CC_2,cdr_names_2_str,TEMP_ABO_CC,PASSWORD_2)
    
    ## Reading CDRs and adding data to DB
    read_files(cdr_names_2)

    ## Deleting temporary directories
    delete_directory(TEMP_SMS_NOTIF)
    delete_directory(TEMP_RADIUS_REQ)
    delete_directory(TEMP_ABO_USSD)
    delete_directory(TEMP_ABO_CC)

    ## Give format to the CSV name list for the SCP connection
    csv_names_tmp=csv_names_days
    while len(csv_names_tmp)>0:
        if len(csv_names_tmp)>MAX_LEN:
            csv_names_2_str=','.join(csv_names_tmp[:MAX_LEN])
            csv_names_tmp=csv_names_tmp[MAX_LEN:]
        else :
            csv_names_2_str=','.join(csv_names_tmp)
            csv_names_tmp=[]
        ## Import CDR files to a temporary directory
        get_csv(USER_1,HOST_CSV_1,PATH_CSV_1,csv_names_2_str,TEMP_CSV,PASSWORD_1)
        #get_csv(USER_2,HOST_CSV_2,PATH_CSV_2,csv_names_2_str,TEMP_CSV,PASSWORD_2)
   
    ## Edit CSV file to replace hour for a timestamp at the beginning of each line (i.e.: '02'->'2022-11-05_02')
    csv_names_tmp=csv_names_days
    edit_csv(csv_names_tmp)
    
    ## Charge CSV files to DB
    csv_names_tmp=csv_names_days
    csv_to_psql(csv_names_tmp)
        
    ## Creation of CVS files
    while len(cdr_names_2)>0:
        cdr_names_tmp=cdr_names_2[:24]
        csv_from_psql('sms_notifications',cdr_names_tmp[0],cdr_names_tmp[-1])
        csv_from_psql('requetes_radius',cdr_names_tmp[0],cdr_names_tmp[-1])
        csv_from_psql('abon_desab_via_ussd',cdr_names_tmp[0],cdr_names_tmp[-1])
        csv_from_psql('abon_desab_via_cc',cdr_names_tmp[0],cdr_names_tmp[-1])
        cdr_names_2=cdr_names_2[24:]
           
    move_file()
    end = time.time()
    my_logger.info('Ending parcon_stats_v1.7, it took {} [s]'.format(str(end - start)))