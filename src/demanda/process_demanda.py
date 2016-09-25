#!/usr/bin/python

import os
import json
import subprocess
import time
import pdb
import psycopg2
import sys
import pickle
import os
import pandas as pd
from datetime import datetime
from openpyxl import load_workbook

from etl_controller import etl

def log_write(New_String):
    print New_String
    if not(os.path.exists(logfile_dir)):
        os.mkdir(logfile_dir)
    filename = str(datetime.now().strftime('%d%m%Y')+ '_demanda.log')

    log_time = str(datetime.now().strftime('%d%m%Y %H:%M:%S') +': ')
    with open(logfile_dir + '/' + filename , 'a+') as fh1:
        fh1.write(log_time + New_String +'\n')
        time.sleep(0.1)

def today_files():
    ls= subprocess.Popen(['ls', '-ltr', Data_Base_dir], stdout=subprocess.PIPE,)
    aws=subprocess.Popen(['awk','{print $9}'],stdin=ls.stdout,stdout=subprocess.PIPE,)
    end_of_pipe = aws.stdout

    arr = [] 
    narr= [] 
    for i in end_of_pipe: 
        arr.append(i)

    for i in arr:
        narr.append(i[0:len(i)-5])
    return narr

def old_files():
    arr = []
    df = pd.read_csv(df_dir+'demanda_df.csv')
    for i in df['file_name']:
        arr.append(i)
    return arr


# controller
if __name__ == "__main__": 

    with open('/home/infografico/coopecg/src/demanda/configuracion.json', 'r') as f:
        data_json = json.load(f)
        Data_Base_dir = data_json["Variable_Database"]["source_path"]
        postgresql_path = data_json["Variable_Database"]["postgresql_connect"]
        logfile_dir=data_json["Variable_Database"]["logfile_path"]
        ctlfile_dir=data_json["Variable_Database"]["control_path"]
        xls_dir = data_json["Variable_Database"]["xls_path"]
        df_dir = data_json["Variable_Database"]["df_path"]

    log_write("Genera dataframe de XLS en el path indicado")
    if not(os.path.exists(df_dir)):
        log_write("ERROR: No se puede accesar la carpeta fuente de DF")
        sys.exit(1)
    else:
        log_write("INFO: Copiando los archivos fuente")
        commx = 'cp -p ' + xls_dir + '*.XLS ' + Data_Base_dir
        os.system(commx)

    old_files = old_files()
    today_files = today_files()
    new_files = list(set(today_files) - set(old_files)) 
   
    for line in new_files:
        a=str(line.strip())
        if (a!= "" and a!="xlsx" and a != "dir"):
            print a
            temp_path = Data_Base_dir + a + '.XLS'
            print "temp path is ", temp_path
            if (os.path.exists(temp_path)):
                libO_command = 'libreoffice --headless --convert-to csv ' + temp_path + ' --outdir ' + Data_Base_dir + 'csv/'
                log_write("INFO: Converting XLS to csv: " + libO_command)
                subprocess.call(libO_command, shell=True)
                #my_local_dict["File_name"]= a
                #pickle.dump(my_local_dict , open(ctlfile_dir + 'demanda.txt' , 'wb'))
            continue

    # clean up headers
    log_write("INFO: cleaning CSV headers")
    ret = os.popen("sed -i 's/\(\"\)\([\d*_[a-zA-Z0-9-]*]*\)\(,[a-zA-Z0-9_]*:Average\"\)/\\2/g' "+Data_Base_dir+"/csv/*.csv ").readlines()

    # substitute commas (,) for points (.) in float values
    log_write("INFO: substitute commas for points in float values")
    ret = os.popen("sed -i 's/\(\"[-]*[0-9]*\)\(,\)\([0-9]*[[:space:]]*\"\)/\\1.\\3/g' "+Data_Base_dir+"/csv/*.csv ").readlines()

    # get rid of all "s
    log_write("INFO: delete all double commands")
    ret = os.popen("sed -i -r 's/\"//g' "+Data_Base_dir+"/csv/*.csv ").readlines()

    # transform all CSV into a data_frame
    log_write("INFO: launching ETL")
    ret = etl(Data_Base_dir+"/csv/")

    # transform all CSV into a data_frame
    log_write("INFO: importing demandas kw data into postgres")
    psql_command = "psql -U postgres postgres://postgres:infografico@172.16.1.101:5432/postgres -f ~/coopecg/src/demanda/import.psql"
    ret = os.popen(psql_command).readlines()
