#!/usr/bin/python

import os
import json
import subprocess
import time
import pdb
import psycopg2
import sys
import os
import pandas as pd
import pickle
from datetime import datetime
from openpyxl import load_workbook

def log_write(New_String):
    print New_String
    if not(os.path.exists(logfile_dir)):
        os.mkdir(logfile_dir)
    filename = str(datetime.now().strftime('%d%m%Y')+ '_demanda'+ '.log')
    log_time = str(datetime.now().strftime('%d%m%Y %H:%M:%S') +': ')
    with open(logfile_dir + '/' + filename , 'a+') as fh1:
        fh1.write(log_time + New_String +'\n')
        time.sleep(0.1)
#VARIABLES
my_dict=[]
my_local_dict={}

query_insert_demanda = "INSERT INTO datos_demanda (\
                name, \
                timestamp , \
                counter) VALUES (\
                %(name)s, \
                %(timestamp)s , \
                %(counter)s)"

with open('/home/infografico/coopecg/src/demanda/configuracion.json', 'r') as f:
    data_json = json.load(f)
    Data_Base_dir = data_json["Variable_Database"]["source_path"] + 'xlsx/'
    postgresql_path = data_json["Variable_Database"]["postgresql_connect"]
    logfile_dir=data_json["Variable_Database"]["logfile_path"]
    ctlfile_dir=data_json["Variable_Database"]["control_path"]
    procesados_dir=data_json["Variable_Database"]["processed_path"]
    xls_dir = data_json["Variable_Database"]["xls_path"]
log_write("Inicializacion del Scrip de Bases de Datos de Demanda de CoopeGuanacaste")

if not(os.path.exists(xls_dir)):
    log_write("ERROR: No se puede accesar la carpeta fuente de XLS")
    sys.exit(1)
else:
    log_write("INFO: Copiando los archivos fuente")
    commy = 'python /home/infografico/coopecg/src/demanda/1demanda_conversion.py'# + xls_dir + '*.XLS ' + Data_Base_dir
    os.system(commy)
    log_write("INFO: Fin del script de conversion Archivos XLS")
#pickle.dump(my_local_dict , open(ctlfile_dir + 'recierres.txt' , 'wb'))
#pdb.set_trace()
my_local_dict=pickle.load(open(ctlfile_dir + 'demanda.txt' , 'rb'))

ls= subprocess.Popen(['ls', '-ltr', Data_Base_dir], stdout=subprocess.PIPE,)
aws=subprocess.Popen(['awk','{print $9}'],stdin=ls.stdout,stdout=subprocess.PIPE,)
end_of_pipe = aws.stdout
