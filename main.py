from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()
# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'

# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema','true').load(file_path)

#imprimimos el esquema
df.printSchema()

# Muestra las primeras filas del DataFrame
df.show()

# Estadisticas básicas
df.summary().show()

#Filtra colegios que tienen más de una sede
print("Colegios con más de una sede\n")
colegios_con_sedes = df.filter(F.col('numero_de_Sedes') > 1).select('nombreestablecimiento', 'numero_de_Sedes',
                                                                    'telefono','correo_Electronico')
colegios_con_sedes.show()

#Ordena colegios por número de sedes en orden descendente
print("Colegios ordenados por número de sedes (mayor a menor)\n")
sorted_df = df.sort(F.col("numero_de_Sedes").desc())
sorted_df.show()

#Muestra el conteo por tipo de establecimiento
print('Distribución de Establecimientos por Tipo:\n')
df.groupBy("tipo_Establecimiento").count().orderBy("count", ascending=False).show()

#Muestra Número de Establecimientos por Zona:
print('Número de Establecimientos por Zona:\n')
df.groupBy("zona").count().orderBy("count", ascending=False).show()

#Análisis de Niveles y Jornadas
print('Conteo de Establecimientos por Niveles y Jornadas:\n')
df.groupBy("niveles", "jornadas").count().orderBy("count", ascending=False).show()

#Número de Establecimientos por Especialidad
print('Conteo de Establecimientos por Especialidad:\n')
df.groupBy("especialidad").count().orderBy("count", ascending=False).show()

#Número de Establecimientos con Capacidades Excepcionales
print('Conteo de stablecimientos con Capacidades Excepcionales:\n')
df.filter(df.capacidades_Excepcionales.isNotNull()).count()

#Distribución de Discapacidades
print('Distribución de Discapacidades:\n')
df.groupBy("discapacidades").count().orderBy("count", ascending=False).show()

#Número de Establecimientos por Prestador de Servicio
print('Número de Establecimientos por Prestador de Servicio:\n')
df.groupBy("prestador_de_Servicio").count().orderBy("count", ascending=False).show()
