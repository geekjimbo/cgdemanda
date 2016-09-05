#/usr/bin/python
#from pypo import *

import json
import csv
import subprocess
import time
import pdb
import psycopg2
import sys
import os
import pickle
from datetime import datetime


def log_write(New_String):
    print New_String
    if not(os.path.exists(logfile_dir)):
        os.mkdir(logfile_dir)
    filename = str(datetime.now().strftime('%d%m%Y')+ '_Satec_CalidadElectrica.log')
    log_time = str(datetime.now().strftime('%d%m%Y %H:%M:%S') +': ')
    with open(logfile_dir + '/' + filename , 'a+') as fh1:
        fh1.write(log_time + New_String +'\n')
        time.sleep(0.1)

def last_record(New_String, filename):
    if not(os.path.exists(ctlfile_dir)):
        os.mkdir(ctrlfile_dir)
    #filename = str(datetime.now().strftime('%d%m%Y')+ '.txt')
    #log_time = str(datetime.now().strftime('%d%m%Y %H:%M:%S') +': ')
    with open(ctlfile_dir + '/' + filename , 'w') as fh1:
        fh1.write(New_String)
        time.sleep(0.01)

#Data_Base_dir = "/home/infografico/coopecg/DataBase/"
#p = pypo()
#array of the data to be inserted in the DB.
#VARIABLES
my_dict=[]
my_local_dict={}

query_insert = "INSERT INTO datos_electricos (\
                name, \
                timestamp , \
                v23, \
                v31, \
                v12,\
                i2_thd, \
                i3_thd, \
                i1_thd, \
                v3_thd, \
                v2_thd, \
                v1_thd, \
                v1, \
                v2, \
                v3, \
                vx_average, \
                unb_v1, \
                unb_v2, \
                unb_v3, \
                max_unbx, \
                vxx_average, \
                unb_v12, \
                unb_v23, \
                unb_v31, \
                max_unbxx) VALUES (\
                %(name)s, \
                %(timestamp)s , \
                %(v23)s, \
                %(v31)s, \
                %(v12)s,\
                %(i2_thd)s, \
                %(i3_thd)s, \
                %(i1_thd)s, \
                %(v3_thd)s, \
                %(v2_thd)s, \
                %(v1_thd)s, \
                %(v1)s, \
                %(v2)s, \
                %(v3)s, \
                %(vx_average)s, \
                %(unb_v1)s, \
                %(unb_v2)s, \
                %(unb_v3)s, \
                %(max_unbx)s, \
                %(vxx_average)s, \
                %(unb_v12)s, \
                %(unb_v23)s, \
                %(unb_v31)s, \
                %(max_unbxx)s)"

#resultado = p.run_sql("select nombre_prov, nombre_canton from infografico.ventas_geo_ta")

#print resultado.head()
#Loading "configuracion.json"
#with open('cosa', 'r') as f:
with open('/home/infografico/coopecg/src/configuracion.json', 'r') as f:
#with open('configuracion.json', 'r') as f:
    data_json = json.load(f)
    Data_Base_dir = data_json["Variable_Database"]["source_path"]
    postgresql_path = data_json["Variable_Database"]["postgresql_connect"]
    logfile_dir=data_json["Variable_Database"]["logfile_path"]
    ctlfile_dir=data_json["Variable_Database"]["control_path"]
    mdb_dir=data_json["Variable_Database"]["init_path"]
    procesados_dir=data_json["Variable_Database"]["processed_path"]
log_write("INFO: Inicializacion del Scrip de Bases de variables Electricas SATEC de CoopeGuanacaste")
#pickle.dump(my_local_dict , open(ctlfile_dir + 'calidad_electrica.txt' , 'wb'))
#pdb.set_trace()
if not(os.path.exists(mdb_dir)):
    log_write("ERROR: No se puede accesar la carpeta de archivos de Medidores Satec. Contacte a Soporte")
    sys.exit(1)
else:
    log_write("INFO: Copiando los archivos fuente")
    #subprocess.call(["cp", (mdb_dir + '*.mdb') ,Data_Base_dir])
    commx = 'cp ' + mdb_dir + '*.mdb ' + Data_Base_dir
    #print commx
    os.system(commx)
#    time.sleep(20)
    commy = Data_Base_dir + 'mybash'
    print commy
    log_write("INFO: Extrayendo tablas de los respectivos medidores")
    #subprocess.call(["source", commy])
    os.system(commy)
    time.sleep(6)

my_local_dict=pickle.load(open(ctlfile_dir + 'calidad_electrica.txt' , 'rb'))

for i in data_json["Barras"]["archivos"]:
    print i["name"]
    my_local_dict=pickle.load(open(ctlfile_dir + 'calidad_electrica.txt' , 'rb'))
    if not(my_local_dict.has_key(i["name"])):
        my_local_dict[(i["name"])]= '0'
        log_write("INFO: Agregando Nombre del Archivo a Control interno de Calidad Electrica: " + i["name"])
    log_write("Lectura de archivo " + i["name"])
    #pdb.set_trace()
    temp_dir=Data_Base_dir + i["name"]
    time.sleep(0.1)
    my_item={}
    if not(os.path.exists(temp_dir)):
        continue
    with open(temp_dir, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for csv_row in reader:
            my_item={}
            my_item["name"]=i["DB_column_name"]
            my_average_list=[]
            my_average_list2=[]
            my_unb_list=[]
            my_unb_list2=[]
#            pdb.set_trace()

            if (float(csv_row["DoubleTime"]) <= float(str(my_local_dict[(i["name"])]))):
#                print "no hay loop"
                continue
          
            for electrical_variable in data_json["Barras"]["variables"]:
                csv_var_name = '{0:s}'.format(electrical_variable["name"])
                DB_var_name ='{0:s}'.format(electrical_variable["DB_column_name"])
                if (csv_var_name == "vx_average"):
                    var_value = float(sum(my_average_list))/float(len(my_average_list))
                    #print "aca va promedio", var_value
                    my_item[DB_var_name]= var_value
                    #print my_item
                    continue
                if (csv_var_name == "unb_v1"):
                    if (float(my_item['vx_average'] == 0)):
                        var_value=0
                    else:
                        var_value=abs((float(my_item['v1'])-float(my_item['vx_average']))/float(my_item['vx_average']))
                    my_item[DB_var_name]= var_value
                    my_unb_list.append(float(var_value))
                    continue
                if (csv_var_name == "unb_v2"):
                    if (float(my_item['vx_average'] == 0)):
                        var_value=0
                    else:
                        var_value=abs((float(my_item['v2'])-float(my_item['vx_average']))/float(my_item['vx_average']))
                    my_item[DB_var_name]= var_value
                    my_unb_list.append(float(var_value))
                    continue
                if (csv_var_name == "unb_v3"):
                    if (float(my_item['vx_average'] == 0)):
                        var_value=0
                    else:
                        var_value=abs((float(my_item['v3'])-float(my_item['vx_average']))/float(my_item['vx_average']))
                    my_item[DB_var_name]= var_value
                    my_unb_list.append(float(var_value))
                    continue
                if (csv_var_name == "max_unbx"):
                    var_value=max(my_unb_list)
                    my_item[DB_var_name]= var_value
                    continue
                if (csv_var_name == "vxx_average"):
                    var_value = float(sum(my_average_list2))/float(len(my_average_list2))
                    #print "aca va promedio", var_value
                    my_item[DB_var_name]= var_value
                    #print my_item
                    continue
                if (csv_var_name == "unb_v12"):
                    if (float(my_item['vxx_average'] == 0)):
                        var_value=0
                    else:
                        var_value=abs((float(my_item['v12'])-float(my_item['vxx_average']))/float(my_item['vxx_average']))
                    my_item[DB_var_name]= var_value
                    my_unb_list2.append(float(var_value))
                    #last_record(my_item['timestamp'], my_item['name'])
                    continue
                if (csv_var_name == "unb_v23"):
                    if (float(my_item['vxx_average'] == 0)):
                        var_value=0
                    else:
                        var_value=abs((float(my_item['v23'])-float(my_item['vxx_average']))/float(my_item['vxx_average']))
                    my_item[DB_var_name]= var_value
                    my_unb_list2.append(float(var_value))
                    continue
                if (csv_var_name == "unb_v31"):
                    if (float(my_item['vxx_average'] == 0)):
                        var_value=0
                    else:
                        var_value=abs((float(my_item['v31'])-float(my_item['vxx_average']))/float(my_item['vxx_average']))
                    my_item[DB_var_name]= var_value
                    my_unb_list2.append(float(var_value))
                    continue
                if (csv_var_name == "max_unbxx"):
                    var_value=max(my_unb_list2)
                    my_item[DB_var_name]= var_value
                    continue
                # Here is where we load the variables out of the CSV
                var_value = csv_row[csv_var_name]
                var_adjust = electrical_variable["adjust"]
                #Keiner: agregado para mejorar precision de horas
                if (var_value  == ""):
                    var_value = 0
                var_value='{0:.3f}'.format((float(var_value)/float(var_adjust)))
                if (DB_var_name == "timestamp"):
                    var_value=int(float(var_value))
                if ((DB_var_name == "v1")|(DB_var_name == "v2")|(DB_var_name=="v3")):
                    my_average_list.append(float(var_value))
                if ((DB_var_name == "v12")|(DB_var_name =="v23")|(DB_var_name=="v31")):
                    my_average_list2.append(float(var_value))
                my_item[DB_var_name]= var_value
#                print my_item
                #print electrical_variable["DB_column_name"], var_value# electrical_variable["adjust"]
            #print my_item
            my_dict.append(my_item)
            my_local_dict[(i["name"])]= my_item['timestamp']

        #print "loop2"
    #print "Loop #1"
    subprocess.call(["cp", (ctlfile_dir + 'calidad_electrica.txt') ,(ctlfile_dir + 'calidad_electrica.txt.old')])
    print ("INFO: Cantidad de datos a ser guardados en DB" , len(my_dict))
    log_write("Escribiendo en Bases de Datos la informacion de " + i["name"] +". Cantidad de lineas: " + str(len(my_dict)))
    #pdb.set_trace()
#
    if len(my_dict) > 0:
        #pdb.set_trace()
        con = psycopg2.connect(postgresql_path)   
        cur = con.cursor()  
        cur.executemany(query_insert, my_dict)
        con.commit()
        log_write("INFO: Cerrando Conexion con DB para "+ i["name"])
        con.close()
    pickle.dump(my_local_dict , open(ctlfile_dir + 'calidad_electrica.txt' , 'wb'))
    log_write("INFO: Guardando informacion local de control para  "+ i["name"])
    my_dict=[]
    subprocess.call(["mv", temp_dir , procesados_dir])
    log_write("INFO: Moviendo archivo a Folder Procesados: " + i["name"])
log_write("INFO: Terminando el Loop de Barras. Continuando con Otros Medidores...")

for i in data_json["Medidores"]["archivos"]:
    print i["name"]
    log_write("INFO: Lectura de archivo " + i["name"])
    my_local_dict=pickle.load(open(ctlfile_dir + 'calidad_electrica.txt' , 'rb'))
    if not(my_local_dict.has_key(i["name"])):
        my_local_dict[(i["name"])]= '0'
        log_write("INFO: Agregando Nombre del Archivo a Control interno de Calidad Electrica: " + i["name"])
    #pdb.set_trace()
    temp_dir=Data_Base_dir + i["name"]
    time.sleep(1)
    my_item={}
    if not(os.path.exists(temp_dir)):
        continue
    with open(temp_dir, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for csv_row in reader:
            my_item={}
            my_item["name"]=i["DB_column_name"]
            my_average_list=[]
            my_average_list2=[]
            my_unb_list=[]
            my_unb_list2=[]
            if (float(csv_row["DoubleTime"]) <= float(str(my_local_dict[(i["name"])]))):
#                print "no hay loop"
                continue
            for electrical_variable in data_json["Medidores"]["variables"]:
                csv_var_name = '{0:s}'.format(electrical_variable["name"])
                DB_var_name ='{0:s}'.format(electrical_variable["DB_column_name"])

                if (csv_var_name == "vx_average"):
                    var_value = float(sum(my_average_list))/float(len(my_average_list))
                    #print "aca va promedio", var_value
                    my_item[DB_var_name]= var_value
                    #print my_item
                    continue
                if (csv_var_name == "unb_v1"):
                    if (float(my_item['vx_average'] == 0)):
                        var_value=0
                    else:
                        var_value=abs((float(my_item['v1'])-float(my_item['vx_average']))/float(my_item['vx_average']))
                    my_item[DB_var_name]= var_value
                    my_unb_list.append(float(var_value))
                    continue
                if (csv_var_name == "unb_v2"):
                    if (float(my_item['vx_average'] == 0)):
                        var_value=0
                    else:
                        var_value=abs((float(my_item['v2'])-float(my_item['vx_average']))/float(my_item['vx_average']))
                    my_item[DB_var_name]= var_value
                    my_unb_list.append(float(var_value))
                    continue
                if (csv_var_name == "unb_v3"):
                    if (float(my_item['vx_average'] == 0)):
                        var_value=0
                    else:
                        var_value=abs((float(my_item['v3'])-float(my_item['vx_average']))/float(my_item['vx_average']))
                    my_item[DB_var_name]= var_value
                    my_unb_list.append(float(var_value))
                    continue
                if (csv_var_name == "max_unbx"):
                    var_value=max(my_unb_list)
                    my_item[DB_var_name]= var_value
                    continue
                if (csv_var_name == "vxx_average"):
                    var_value = float(sum(my_average_list2))/float(len(my_average_list2))
                    #print "aca va promedio", var_value
                    my_item[DB_var_name]= var_value
                    #print my_item
                    continue
                if (csv_var_name == "unb_v12"):
                    if (float(my_item['vxx_average'] == 0)):
                        var_value=0
                    else:
                        var_value=abs((float(my_item['v12'])-float(my_item['vxx_average']))/float(my_item['vxx_average']))
                    my_item[DB_var_name]= var_value
                    my_unb_list2.append(float(var_value))
                   # last_record(my_item['timestamp'], my_item['name'])
                    continue
                if (csv_var_name == "unb_v23"):
                    if (float(my_item['vxx_average'] == 0)):
                        var_value=0
                    else:
                        var_value=abs((float(my_item['v23'])-float(my_item['vxx_average']))/float(my_item['vxx_average']))
                    my_item[DB_var_name]= var_value
                    my_unb_list2.append(float(var_value))
                    continue
                if (csv_var_name == "unb_v31"):
                    if (float(my_item['vxx_average'] == 0)):
                        var_value=0
                    else:
                        var_value=abs((float(my_item['v31'])-float(my_item['vxx_average']))/float(my_item['vxx_average']))
                    my_item[DB_var_name]= var_value
                    my_unb_list2.append(float(var_value))
                    continue
                if (csv_var_name == "max_unbxx"):
                    var_value=max(my_unb_list2)
                    my_item[DB_var_name]= var_value
                    continue
                ## Here is where we load the variables`Y
                var_value = csv_row[csv_var_name]
                var_adjust = electrical_variable["adjust"]
                if (var_value  == ""):
                    var_value = 0
                var_value='{0:.5f}'.format((float(var_value)/float(var_adjust)))
                if ((DB_var_name == "v1")|(DB_var_name == "v2")|(DB_var_name=="v3")):
                    my_average_list.append(float(var_value))
                if ((DB_var_name == "v12")|(DB_var_name =="v23")|(DB_var_name=="v31")):
                    my_average_list2.append(float(var_value))
                my_item[DB_var_name]= var_value
#                print my_item
                #print electrical_variable["DB_column_name"], var_value# electrical_variable["adjust"]
            #print my_item
            my_dict.append(my_item)
            my_local_dict[(i["name"])]= my_item['timestamp']
#    print my_dict
#    print "Loop #1"
    subprocess.call(["cp", (ctlfile_dir + 'calidad_electrica.txt') ,(ctlfile_dir + 'calidad_electrica.txt.old')])
    print ("INFO: Cantidad de datos a ser guardados en DB" , len(my_dict))
    log_write("Escribiendo en Bases de Datos la informacion de "+i["name"] +". Cantidad de lineas: " + str(len(my_dict)))
    #pdb.set_trace()
    if len(my_dict) > 0:
        #pdb.set_trace()
        con = psycopg2.connect(postgresql_path)   
        cur = con.cursor()  
        cur.executemany(query_insert, my_dict)
        con.commit()
        con.close()
        log_write("Cerrando Conexion con DB para "+ i["name"])
    pickle.dump(my_local_dict , open(ctlfile_dir + 'calidad_electrica.txt' , 'wb'))
    log_write("INFO: Guardando informacion local de control para  "+ i["name"])
    my_dict=[]
    subprocess.call(["mv", temp_dir , procesados_dir])
    log_write("INFO: Moviendo archivo a Folder Procesados: " + i["name"])
    log_write("INFO: Cerrando Script de Procesamiento de variables de Calidad Electrica")


#            for electrical_variable in data_json["Barras"]["variables"]:
#            print var_name
#                if not(csv_row[electrical_variable["name"]] == "") :
#                    temp='{0:.3f}'.format((float(csv_row[electrical_variable["name"]])/float(electrical_variable["adjust"])))
#                #print("variable: " , '{0:s}'.format(csv_row[electrical_variable["DB_column_name"]), " ", temp)
#                    print(temp)
#for i in data_json["Barras"]["variables"]:
#    print i["name"], i["adjust"]
	#print i["adjust"]

#for i in data_json["team"]:
#	print i["team_name"]
#
#print json.dumps(config_json)
#print
#print config_json["Medidores"]["variables"]
#
#try:
#     
#    log_write("Escribiendo en Bases de Datos informacion sobre las Barras. Cantidad de lineas: " + str(len(my_dict)))
#    con = psycopg2.connect(postgresql_path)   
#    cur = con.cursor()  
##    
##    cur.execute("DROP TABLE IF EXISTS datos_electricos")
##    cur.execute(query_create)
##    cur.execute("CREATE TABLE datos_electricos(Id INT PRIMARY KEY, Name TEXT, Price INT)")
##    query = "INSERT INTO datos_electricos (Id, Name, Price) VALUES (%s, %s, %s)"
#    cur.executemany(query_insert, my_dict)
##    cur.executemany(query_insert, cars)
#        
#    con.commit()
#    
#
#except psycopg2.DatabaseError, e:
#    
#    if con:
#        con.rollback()
#    print 'Error %s' % e    
#    log_write('Error %s' %e)
#    sys.exit(1)
#    
#    
#finally:
##    if con:
##    con.close()
#    log_write("Cerrando Conexion con DB")

