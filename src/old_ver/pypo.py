import pandas as pd
import psycopg2 as pg
import pandas.io.sql as psql
import os
import time
from datetime import datetime

logfile_dir="/home/infografico/coopecg/log/"
class pypo():
    def __init__(self):
       self.conn = pg.connect("dbname=postgres host=172.16.1.101 port=5432 user=postgres password=infografico")

    def run_sql(self, mquery):
       resultado = psql.read_sql(mquery, self.conn)
       return resultado

    def log_write(New_String):
        if not(os.path.exists(logfile_dir)):
            os.mkdir(logfile_dir)
        filename = str(datetime.now().strftime('%d%m%Y'))
        log_time = str(datetime.now().strftime('%d%m%Y %H:%M:%S') +': ')
        with open(logfile_dir + '/' + filename , 'a+') as fh1:
            fh1.write(log_time + New_String +'\n')
            time.sleep(0.1)

    def file_count(file_):
        with open(file_) as f:
            count = sum(1 for _ in f)
        return count

    def data_write(New_String):
        if not(os.path.exists(datafile_dir)):
            os.mkdir(datafile_dir)
        log_time = str(datetime.now().strftime('%d%m%Y %H:%M:%S'))
        with open(datafile_dir + '/' + data_filename , 'a+') as fh2:
            fh2.write(log_time + ',' + New_String +'\n')
            time.sleep(0.1)
