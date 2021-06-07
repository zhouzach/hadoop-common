
from pyspark.sql import SparkSession
from pyspark.sql.types import *

org.rabbit.spark = SparkSession \
    .builder \
    .appName("python org.rabbit.spark job") \
    .enableHiveSupport() \
    .getOrCreate()

cSchema = StructType([StructField("Words", StringType())\
                      ,StructField("total", IntegerType())])

test_list = [['Hello', 1], ['I am fine', 3]]

df = org.rabbit.spark.createDataFrame(test_list,cSchema)

df.show()
