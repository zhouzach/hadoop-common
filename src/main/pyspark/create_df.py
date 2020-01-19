
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("python spark job") \
    .enableHiveSupport() \
    .getOrCreate()

cSchema = StructType([StructField("Words", StringType())\
                      ,StructField("total", IntegerType())])

test_list = [['Hello', 1], ['I am fine', 3]]

df = spark.createDataFrame(test_list,cSchema)

df.show()
