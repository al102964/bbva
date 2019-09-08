from pyspark.sql import Window
from pyspark.sql.functions import row_number, desc, input_file_name
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import SparkContext
from pyspark.sql.types import *

#sc = SparkContext("spark://personal-Lenovo-G50-80:7077", "First App")
sc = SparkContext(appName="hackaton")
spark = SparkSession(sc)

spark  = SparkSession.builder.getOrCreate()
#SparkSession.builder.config(conf=conf).getOrCreate()

#files = ['s3://al102964-bucket1/bbva/origen_corregido.csv','s3://al102964-bucket1/bbva/update_corregido.csv']

#files = ['hdfs://ec2-34-221-103-83.us-west-2.compute.amazonaws.com:8020/user/root/bbva/origen_corregido.csv','hdfs://ec2-34-221-103-83.us-west-2.compute.amazonaws.com:8020/user/root/bbva/update_corregido.csv']
files = ["input/MCDTINT_PIPES_HIST.txt","input/MCDTINT_PIPES_UPD.txt"]


schema = StructType([StructField('Fecha_alta_aclaracion', StringType(), True),\
                     StructField('Folio', StringType(), True),\
                     StructField('BIN', StringType(), True),\
                     StructField('TARJETA', StringType(), True),\
                     StructField('control', StringType(), True),\
                     StructField('secuencia', StringType(), True),\
                     StructField('transaccion', StringType(), True),\
                     StructField('importe', StringType(), True),\
                     StructField('indicador', StringType(), True),\
                     StructField('indicador2', StringType(), True)])

df = spark.read.csv(files,header=True,schema=schema,sep="|").withColumn("_ifn", input_file_name())

key_list = ["Fecha_alta_aclaracion","Folio","BIN","TARJETA","control","secuencia"]

w = Window.partitionBy([col(x) for x in key_list]).orderBy(desc('_ifn'))
df = df.withColumn("_rn", row_number().over(w)).filter(col("_rn") != 2)


df.coalesce(1)\
      .write\
      .option("header","true")\
      .option("sep",",")\
      .mode("overwrite")\
      .csv("output/")



"""
df.coalesce(1)\
      .write\
      .option("header","true")\
      .option("sep",",")\
      .mode("overwrite")\
      .csv("hdfs://ec2-34-221-103-83.us-west-2.compute.amazonaws.com:8020/user/root/bbva/salida")
"""

#key_list.append("_rn")
#key_list.append("_ifn")

# grab only the rows that were first (most recent) in each window
# clean up working columns
#df = df.where(df._rn == 2).drop("_rn").drop("_ifn")

#print(df.filter(col("_rn") != 2).count())
#print(df.filter(col("_ifn") == "file:///home/personal/hackaton_bbva/update_corregido.csv").count())

#print(df.filter(col("_ifn") != "file:///home/personal/hackaton_bbva/update_corregido.csv").count())
#df.select(key_list).show(truncate=False)

#df.filter(col("_rn") != 2).filter(col("_rn") != 1).show()

#print(df.filter(col("_rn") != 2).count())

#print(df.count())

#df.show()
