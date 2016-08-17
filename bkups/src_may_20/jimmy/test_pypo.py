from pypo import pypo

p = pypo()

resultado = p.run_sql("select nombre_prov, nombre_canton from infografico.ventas_geo_ta")

print resultado.head()

