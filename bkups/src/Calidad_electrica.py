#/usr/bin/python
from pypo import *
import json
import csv
import time

#Data_Base_dir = "/home/infografico/coopecg/DataBase/"
#p = pypo()

#resultado = p.run_sql("select nombre_prov, nombre_canton from infografico.ventas_geo_ta")

#print resultado.head()


#Loading "configuracion.json"
#with open('cosa', 'r') as f:
with open('configuracion.json', 'r') as f:
    data_json = json.load(f)
    Data_Base_dir = data_json["Variable_Database"]["source_path"]
for i in data_json["Barras"]["archivos"]:
    print i["name"]
    temp_dir=Data_Base_dir + i["name"]
    time.sleep(5)
    if not(os.path.exists(temp_dir)):
        continue
    with open(temp_dir, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for csv_row in reader:
            for electrical_variable in data_json["Barras"]["variables"]:
                print electrical_variable["name"], electrical_variable["adjust"]
                if not(csv_row[electrical_variable["name"]] == "") :
                    temp='{0:.3f}'.format((float(csv_row[electrical_variable["name"]])/float(electrical_variable["adjust"])))
                #print("variable: " , '{0:s}'.format(csv_row[electrical_variable["DB_column_name"]), " ", temp)
                    print(temp)
#for i in data_json["Barras"]["variables"]:
#    print i["name"], i["adjust"]
	#print i["adjust"]

#for i in data_json["team"]:
#	print i["team_name"]
#
#print json.dumps(config_json)
#print
#print config_json["Medidores"]["variables"]

