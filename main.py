#Importa las librerías necesarias: Se importa SparkSession para crear una sesión de Spark y functions como F para
#utilizar funciones de PySpark en operaciones sobre DataFrames.
from pyspark.sql import SparkSession, functions as F

# Crea una sesión de Spark: Se inicializa una nueva sesión de Spark con el nombre "Tarea3".
# Si ya existe una sesión con ese nombre, se utiliza la existente.
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

#Define la ruta del archivo CSV: Aquí se especifica la ruta del archivo CSV que se va a cargar.
#file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'
file_path = 'LISTADO_COLEGIOS_BOGOTA_20241019.csv'

# Carga el archivo CSV en un DataFrame
df = spark.read.format('csv').option('header','true').option('inferSchema','true').load(file_path)

#Imprime el esquema del DataFrame
df.printSchema()

# Muestra las primeras filas
df.show()

# Genera estadísticas básicas
df.summary().show()

# Filtra colegios con más de una sede
print("Colegios con más de una sede\n")
colegios_con_sedes = df.filter(F.col('numero_de_Sedes') > 1).select('nombreestablecimiento', 'numero_de_Sedes',
                                                                    'telefono','correo_Electronico')
colegios_con_sedes.show()

# Ordena colegios por número de sedes
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
