from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, dense_rank, expr
from pyspark.sql.types import StringType, StructType, StructField, DoubleType
from pyspark.sql.functions import from_json
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

# Create Spark Session
spark = SparkSession \
    .builder \
    .appName("Batch Processing") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()

# Define the PySpark schema
json_schema = StructType([
    StructField('key', StringType(), True),
    StructField('name', StringType(), True),
    StructField('date', StringType(), True),
    StructField('value', DoubleType(), True),
    StructField('description', StringType(), True)
])

# Read data from Kafka topic, starting from the latest offset
batch_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "empresas") \
    .load()

# Parse JSON data
json_df = batch_df.selectExpr("cast(value as string) as value")
batch_data = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*")

# Ordena el DataFrame por la columna "date" de forma descendente y selecciona los últimos 10 registros
agg_data = batch_data.groupBy("description").agg(expr("max(date) as max_date"), expr("last(value, true) as last_value"), expr("last(name, true) as name"))

df_actual = agg_data.orderBy(expr("last_value").desc())


#Cargo los datos iniciales con los que voy a calcular la variación de los valores de las acciones

# df_actual = agg_data.orderBy(expr("last_value").desc()).limit(10)

# # Write the result to Parquet files
# df_actual.write \
#     .mode("overwrite") \
#     .parquet("./datos_iniciales/")

# df_actual.show()



    # Crear la sesión de Spark
spark = SparkSession.builder.appName("Lectura de Parquet").getOrCreate()

#     # Leer el archivo Parquet
df_pivot = spark.read.parquet("./datos_iniciales/")


#     # Mostrar el resultado en pantalla

# Rename columns
df_pivot = df_pivot.withColumnRenamed("last_value", "valor_actual")
df_pivot = df_pivot.withColumnRenamed("max_date", "initial_date")
df_pivot = df_pivot.withColumnRenamed("description", "initial_description")

df_actual = df_actual.withColumnRenamed("last_value", "valor_inicial")
df_actual = df_actual.withColumnRenamed("max_date", "actual_date")
df_actual = df_actual.withColumnRenamed("name", "actual_name")

# df_pivot.show()
# df_actual.show()

# Compare the difference between the two columns named "last_value" for each row
difference_df = df_pivot.join(
    df_actual,
    df_pivot["name"] == df_actual["actual_name"],  # Specify a common column for joining
    "inner"
).withColumn("difference", df_pivot["valor_actual"] - df_actual["valor_inicial"])

# Calculate the percentage difference
difference_df = difference_df.withColumn("difference_percentage", (difference_df["difference"] / difference_df["valor_inicial"]) * 100)

# Show the result

# Agregar una columna de fecha formateada
difference_df = difference_df.withColumn("formatted_date", date_format(difference_df["actual_date"], "yyyyMMdd"))

# Escribir el resultado en un archivo Parquet
# difference_df.write \
#     .partitionBy("formatted_d ate") \
#     .mode("overwrite") \
#     .parquet("./resultado/")

difference_df.select("initial_description", "initial_date", "formatted_date", "valor_inicial", "valor_actual", "difference", "difference_percentage").write \
    .partitionBy("formatted_date") \
    .mode("overwrite") \
    .parquet("./resultado/")

difference_df.select("initial_description", "initial_date", "formatted_date", "valor_inicial", "valor_actual", "difference", "difference_percentage").show()

total_percentage = difference_df.agg({"difference_percentage": "sum"}).collect()[0][0]

capital_inicial = 1000000
capital_por_accion = capital_inicial / 10
percentage_per_action = total_percentage / 10
gana_por_accion = capital_por_accion * percentage_per_action / 100

ganancia_total = gana_por_accion * 10
print("\n\n\n\n\n\n\n\n\n")
print("El total de la variación porcentual es: {:.2f}% por acción. (en promedio)".format(total_percentage/10))
print(f"La ganancia es de: {ganancia_total:.2f}  pesos.")