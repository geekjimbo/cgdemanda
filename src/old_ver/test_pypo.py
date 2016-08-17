#/usr/bin/python
from pypo import pypo

p = pypo()

#resultado = p.run_sql("select * from datos_electricos")

#print resultado.head()
#resultado = p.run_sql("select max_unbx, vxx_average, unb_v12, unb_v23, unb_v31, max_unbxx from datos_electricos")
#print resultado.head()

resultado = p.run_sql("select max_unbx, vxx_average, unb_v12, unb_v23, unb_v31, max_unbxx from datos_electricos \
                        where name= 'clinica_filadelfia' \
                        group by name, max_unbx, vxx_average, unb_v12, unb_v23, unb_v31, max_unbxx ")
#print resultado.head()
resultado = p.run_sql("select count(timestamp) from datos_electricos")
print "cantidad de rows: ", resultado.head()

resultado = p.run_sql("select name, (max(timestamp/1000)) from datos_electricos where name = 'barra_a_guayabal' \
                        group by name")
print "cantidad de rows: ", resultado.head()

resultado = p.run_sql("SELECT name, to_timestamp(timestamp/1000) FROM datos_electricos \
                    WHERE (name, timestamp) IN (\
                    SELECT name, MAX(timestamp) FROM datos_electricos GROUP BY name)\
                    limit 50")
print "cantidad de rows: ", resultado.head()
