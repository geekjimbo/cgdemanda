#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import sys

kmr2= [
{'v31': '215.392', 'unb_v2': 0.0038935444072359373, 'unb_v3': 0.009589655669673637, 'i1_thd': '0.000', 'unb_v1': 0.005696111262437699, 'v12': '217.633', 'v3_thd': '0.000', 'i2_thd': '0.000', 'vxx_average': 216.136, 'v2_thd': '0.000', 'unb_v23': 0.003483917533404828, 'v23': '215.383', 'vx_average': '124.822', 'i3_thd': '0.000', 'timestamp': '1454371200000.000', 'v1_thd': '0.000', 'max_unbxx': 0.006926194618203419, 'v1': '125.533', 'v2': '125.308', 'v3': '123.625', 'unb_v12': 0.006926194618203419, 'max_unbx': 0.009589655669673637, 'name': u'barra_a_guayabal', 'unb_v31': 0.003442277084798459},
{'v31': '1.743', 'unb_v2': 0.0006609385327163846, 'unb_v3': 0.0006609385327163846, 'i1_thd': '0.000', 'unb_v1': 0.0013218770654329895, 'v12': '1.750', 'v3_thd': '0.000', 'i2_thd': '0.000', 'vxx_average': 1.7460000000000002, 'v2_thd': '0.000', 'unb_v23': 0.000572737686139812, 'v23': '1.745', 'vx_average': 1.0086666666666666, 'i3_thd': '0.000', 'timestamp': '1454371800000.000', 'v1_thd': '0.000', 'max_unbxx': 0.0022909507445588667, 'v1': '1.010', 'v2': '1.008', 'v3': '1.008', 'unb_v12': 0.0022909507445588667, 'max_unbx': 0.0013218770654329895, 'name': 'barra_a_guayabal', 'unb_v31': 0.0017182130584193088}, {'v31': '1.762', 'unb_v2': 0.004259501965924169, 'unb_v3': 0.00032765399737895017, 'i1_thd': '0.000', 'unb_v1': 0.004587155963302683, 'v12': '1.770', 'v3_thd': '0.000', 'i2_thd': '0.000', 'vxx_average': 1.7613333333333336, 'v2_thd': '0.000', 'unb_v23': 0.005299015897047863, 'v23': '1.752', 'vx_average': 1.0173333333333334, 'i3_thd': '0.000', 'timestamp': '1454382600005.000', 'v1_thd': '0.000', 'max_unbxx': 0.005299015897047863, 'v1': '1.022', 'v2': '1.013', 'v3': '1.017', 'unb_v12': 0.00492051476154412, 'max_unbx': 0.004587155963302683, 'name': 'barra_a_guayabal', 'unb_v31': 0.0003785011355032387}, {'v31': '1.762', 'unb_v2': 0.010794896957801668, 'unb_v3': 0.006869479882237604, 'i1_thd': '0.000', 'unb_v1': 0.003925417075564283, 'v12': '1.765', 'v3_thd': '3.700', 'i2_thd': '0.000', 'vxx_average': 1.7636666666666667, 'v2_thd': '3.400', 'unb_v23': 0.00018900018900016818, 'v23': '1.764', 'vx_average': 1.019, 'i3_thd': '0.000', 'timestamp': '1454383200008.000', 'v1_thd': '3.600', 'max_unbxx': 0.0009450009450009668, 'v1': '1.023', 'v2': '1.008', 'v3': '1.026', 'unb_v12': 0.0007560007560006727, 'max_unbx': 0.010794896957801668, 'name': 'barra_a_guayabal', 'unb_v31': 0.0009450009450009668}, {'v31': '1.757', 'unb_v2': 0.005573770491803321, 'unb_v3': 0.0022950819672130805, 'i1_thd': '0.000', 'unb_v1': 0.00327868852459024, 'v12': '1.765', 'v3_thd': '2.100', 'i2_thd': '0.000', 'vxx_average': 1.7606666666666666, 'v2_thd': '2.100', 'unb_v23': 0.0003786444528587239, 'v23': '1.760', 'vx_average': 1.0166666666666666, 'i3_thd': '0.000', 'timestamp': '1454383800009.000', 'v1_thd': '2.000', 'max_unbxx': 0.002461188943581958, 'v1': '1.020', 'v2': '1.011', 'v3': '1.019', 'unb_v12': 0.002461188943581958, 'max_unbx': 0.005573770491803321, 'name': 'barra_a_guayabal', 'unb_v31': 0.0020825444907232337},
{'v31': '1.750', 'unb_v2': 0.005263157894736919, 'unb_v3': 0.0016447368421050818, 'i1_thd': '0.000', 'unb_v1': 0.0036184210526313994, 'v12': '1.761', 'v3_thd': '2.200', 'i2_thd': '0.000', 'vxx_average': 1.7546666666666664, 'v2_thd': '2.100', 'unb_v23': 0.0009498480243160049, 'v23': '1.753', 'vx_average': 1.0133333333333334, 'i3_thd': '0.000', 'timestamp': '1454384400005.000', 'v1_thd': '2.100', 'max_unbxx': 0.003609422492401325, 'v1': '1.017', 'v2': '1.008', 'v3': '1.015', 'unb_v12': 0.003609422492401325, 'max_unbx': 0.005263157894736919, 'name': 'barra_a_guayabal', 'unb_v31': 0.0026595744680849407},
{'v31': '1.750', 'unb_v2': 0.002300361485376239, 'unb_v3': 0.004272099901413046, 'i1_thd': '0.000', 'unb_v1': 0.006572461386789285, 'v12': '1.762', 'v3_thd': '2.300', 'i2_thd': '0.000', 'vxx_average': 1.7566666666666666, 'v2_thd': '2.600', 'unb_v23': 0.0007590132827324906, 'v23': '1.758', 'vx_average': 1.0143333333333333, 'i3_thd': '0.000', 'timestamp': '1454397600001.000', 'v1_thd': '2.200', 'max_unbxx': 0.0037950664136622006, 'v1': '1.021', 'v2': '1.012', 'v3': '1.010', 'unb_v12': 0.0030360531309298363, 'max_unbx': 0.006572461386789285, 'name': 'barra_a_guayabal', 'unb_v31': 0.0037950664136622006},
{'v31': '1.745', 'unb_v2': 0.005595786701777527, 'unb_v3': 0.0032916392363397736, 'i1_thd': '0.000', 'unb_v1': 0.0023041474654377538, 'v12': '1.758', 'v3_thd': '2.600', 'i2_thd': '0.000', 'vxx_average': 1.7536666666666667, 'v2_thd': '2.800', 'unb_v23': 0.0024710131153772856, 'v23': '1.758', 'vx_average': 1.0126666666666666, 'i3_thd': '0.000', 'timestamp': '1454398200004.000', 'v1_thd': '2.500', 'max_unbxx': 0.004942026230754571, 'v1': '1.015', 'v2': '1.007', 'v3': '1.016', 'unb_v12': 0.0024710131153772856, 'max_unbx': 0.005595786701777527, 'name': 'barra_a_guayabal', 'unb_v31': 0.004942026230754571}, 
{'v31': '1.740', 'unb_v2': 0.003964321110010134, 'unb_v3': 0.0009910802775025885, 'i1_thd': '0.000', 'unb_v1': 0.004955401387512283, 'v12': '1.753', 'v3_thd': '2.400', 'i2_thd': '0.000', 'vxx_average': 1.7469999999999999, 'v2_thd': '2.700', 'unb_v23': 0.0005724098454494059, 'v23': '1.748', 'vx_average': 1.0090000000000001, 'i3_thd': '0.000', 'timestamp': '1454398800007.000', 'v1_thd': '2.300', 'max_unbxx': 0.004006868918145333, 'v1': '1.014', 'v2': '1.005', 'v3': '1.008', 'unb_v12': 0.0034344590726960536, 'max_unbx': 0.004955401387512283, 'name': u'barra_a_guayabal', 'unb_v31': 0.004006868918145333},
{'v31': '1.748', 'unb_v2': 0.0019743336623889458, 'unb_v3': 0.002961500493583309, 'i1_thd': '0.000', 'unb_v1': 0.004935834155972474, 'v12': '1.760', 'v3_thd': '0.000', 'i2_thd': '0.000', 'vxx_average': 1.7546666666666668, 'v2_thd': '0.000', 'unb_v23': 0.0007598784194528038, 'v23': '1.756', 'vx_average': 1.013, 'i3_thd': '0.000', 'timestamp': '1454399400008.000', 'v1_thd': '0.000', 'max_unbxx': 0.003799392097264525, 'v1': '1.018', 'v2': '1.011', 'v3': '1.010', 'unb_v12': 0.003039513677811468, 'max_unbx': 0.004935834155972474, 'name': u'barra_a_guayabal', 'unb_v31': 0.003799392097264525},
{'v31': '1.747', 'unb_v2': 0.007912957467853619, 'unb_v3': 0.004945598417408621, 'i1_thd': '0.000', 'unb_v1': 0.0029673590504452167, 'v12': '1.752', 'v3_thd': '0.000', 'i2_thd': '0.000', 'vxx_average': 1.752, 'v2_thd': '0.000', 'unb_v23': 0.002853881278538752, 'v23': '1.757', 'vx_average': 1.011, 'i3_thd': '0.000', 'timestamp': '1454400000003.000', 'v1_thd': '0.000', 'max_unbxx': 0.002853881278538752, 'v1': '1.014', 'v2': '1.003', 'v3': '1.016', 'unb_v12': 0.0, 'max_unbx': 0.007912957467853619, 'name': u'barra_a_guayabal', 'unb_v31': 0.002853881278538752}
    ]
kmr3= [{'v31': '215.392', 'unb_v2': '0.0038935444072359373', 'unb_v3': '0.009589655669673637', 'i1_thd': '0.000', 'unb_v1': '0.005696111262437699', 'v12': '217.633', 'v3_thd': '0.000', 
'i2_thd': '0.000', 'vxx_average': '216.136', 'v2_thd': '0.000', 'unb_v23': '0.003483917533404828', 'v23': '215.383', 
'vx_average': '124.822', 'i3_thd': '0.000', 'timestamp': '1454371200000.000', 'v1_thd': '0.000', 
'max_unbxx': '0.006926194618203419', 'v1': '125.533', 'v2': '125.308', 'v3': '123.625', 
'unb_v12': '0.006926194618203419', 'max_unbx': '0.009589655669673637', 'name': 'barra_a_guayabal', 'unb_v31': '0.003442277084798459'}]

cars = [{'v23': '213.200', 'v31': '212.400', 'i2_thd': '0.000','name': "cosa1", 
'i3_thd':'0.000', 'i1_thd': '0.000', 'v12': '213.900', 'timestamp': '1454371200000.000',
'v3_thd': '0.000', 'v2_thd': '0.000', 'v1_thd': '0.000', 'v1': '1.032', 'v2':'1.024', 
'v3': '1.022'}, {'name': "cosa2", 'v23': '209.400', 'v31': '209.200', 'i2_thd':'0.000', 
'i3_thd': '0.000', 'i1_thd': '0.000', 'v12': '210.000', 'timestamp': '1454371800000.000', 
'v3_thd': '0.000', 'v2_thd': '0.000', 'v1_thd': '0.000',
'v1': '1.010', 'v2': '1.008', 'v3': '1.008'}, { 'v23': '210.200', 'name': "cosa12",'v31':'211.500', 
'i2_thd': '0.000', 'i3_thd': '0.000', 'i1_thd': '0.000', 'v12': '212.400',
'timestamp': '1454382600005.000', 'v3_thd': '0.000', 'v2_thd': '0.000', 
'v1_thd': '0.000', 'v1': '1.022', 'v2': '1.013', 'v3': '1.017'},
{'name': "cosa", 'v23': '211.700', 'v31': '211.500', 'i2_thd': '0.000', 'i3_thd': '0.000',
'i1_thd': '0.000', 'v12': '211.800', 'timestamp': '1454383200008.000',
'v3_thd': '3.700', 'v2_thd': '3.400', 'v1_thd': '3.600', 'v1': '1.023', 'v2': '1.008',
'v3': '1.026'}, {'name': "cosa3", 'v23': '211.200', 'v31': '210.800', 'i2_thd':'0.000', 
'i3_thd': '0.000', 'i1_thd': '0.000', 'v12': '211.800', 'timestamp': '1454383800009.000',
'v3_thd': '2.100', 'v2_thd': '2.100', 'v1_thd': '2.000',
'v1': '1.020', 'v2': '1.011', 'v3': '1.019'}, {'name': "cosa", 'v23': '210.300', 'v31': '210.000', 
'i2_thd': '0.000', 'i3_thd': '0.000', 'i1_thd': '0.000', 'v12':'211.300', 
'timestamp': '1454384400005.000', 'v3_thd': '2.200', 'v2_thd': '2.100', 
'v1_thd': '2.100', 'v1': '1.017', 'v2': '1.008', 'v3': '1.015'},
{'name': "cosa", 'v23': '210.900', 'v31': '210.000', 'i2_thd': '0.000', 'i3_thd': '0.000',
'i1_thd': '0.000', 'v12': '211.400', 'timestamp': '1454397600001.000',
'v3_thd': '2.300', 'v2_thd': '2.600', 'v1_thd': '2.200', 'v1': '1.021', 'v2': '1.012', 
'v3': '1.010'}, {'name': "cosa", 'v23': '210.900', 'v31': '209.400', 'i2_thd': '0.000', 
'i3_thd': '0.000', 'i1_thd': '0.000', 'v12': '211.000', 'timestamp':
'1454398200004.000', 'v3_thd': '2.600', 'v2_thd': '2.800', 'v1_thd': '2.500',
'v1': '1.015', 'v2': '1.007', 'v3': '1.016'}, {'name': "cosa4", 'v23': '209.800', 'v31':'208.800', 
'i2_thd': '0.000', 'i3_thd': '0.000', 'i1_thd': '0.000', 'v12': '210.400', 
'timestamp': '1454398800007.000', 'v3_thd': '2.400', 'v2_thd': '2.700', 
'v1_thd': '2.300', 'v1': '1.014', 'v2': '1.005', 'v3': '1.008'},
{'name': "cosa", 'v23': '210.700', 'v31': '209.800', 'i2_thd': '0.000', 'i3_thd': '0.000',
'i1_thd': '0.000', 'v12': '211.200', 'timestamp': '1454399400008.000',
'v3_thd': '0.000', 'v2_thd': '0.000', 'v1_thd': '0.000', 'v1': '1.018', 'v2': '1.011', 
'v3': '1.010'}, {'name': "cosa", 'v23': '210.800', 'v31': '209.600', 'i2_thd': '0.000', 
'i3_thd': '0.000', 'i1_thd': '0.000', 'v12': '210.200', 'timestamp': '1454400000003.000', 
'v3_thd': '0.000', 'v2_thd': '0.000', 'v1_thd': '0.000',
'v1': '1.014', 'v2': '1.003', 'v3': '1.016'}]



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

#                id INT PRIMARY KEY, \
query_create = "CREATE TABLE datos_electricos(\
                name CHAR(100), \
                timestamp REAL, \
                v23 REAL, \
                v31 REAL, \
                v12 REAL,\
                i2_thd REAL, \
                i3_thd REAL, \
                i1_thd REAL, \
                v3_thd REAL, \
                v2_thd REAL, \
                v1_thd REAL, \
                v1 REAL, \
                v2 REAL, \
                v3 REAL, \
                vx_average REAL, \
                unb_v1 REAL, \
                unb_v2 REAL, \
                unb_v3 REAL, \
                max_unbx REAL, \
                vxx_average REAL, \
                unb_v12 REAL, \
                unb_v23 REAL, \
                unb_v31 REAL, \
                max_unbxx REAL)"
con = None

try:
     
    con = psycopg2.connect("dbname=postgres host=172.16.1.101 port=5432 user=postgres password=infografico")   
  
    cur = con.cursor()  
    
    cur.execute("DROP TABLE IF EXISTS datos_electricos")
    cur.execute(query_create)
#    cur.execute("CREATE TABLE datos_electricos(Id INT PRIMARY KEY, Name TEXT, Price INT)")
#    query = "INSERT INTO datos_electricos (Id, Name, Price) VALUES (%s, %s, %s)"
#    cur.executemany(query_insert, kmr2)
#    cur.executemany(query_insert, cars)
        
    con.commit()
    

except psycopg2.DatabaseError, e:
    
    if con:
        con.rollback()
    
    print 'Error %s' % e    
    sys.exit(1)
    
    
finally:
    
    if con:
        con.close()



