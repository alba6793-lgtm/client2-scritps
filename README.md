# client2-scritps
Client 2 scripts
##  parcon_stats.py
##  author  : CA
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
