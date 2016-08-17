import pandas as pd
import psycopg2 as pg
import pandas.io.sql as psql

class pypo():
    def __init__(self):
       self.conn = pg.connect("dbname=postgres host=172.16.1.101 port=5432 user=postgres password=infografico")

    def run_sql(self, mquery):
       resultado = psql.read_sql(mquery, self.conn)
       return resultado
