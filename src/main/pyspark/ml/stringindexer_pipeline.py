from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer

spark = SparkSession \
    .builder \
    .appName("python spark job") \
    .enableHiveSupport() \
    .getOrCreate()

cSchema = StructType([StructField("id", IntegerType()),
                      StructField("category", StringType()),
                      StructField("value", StringType())
                      ])

test_list = [[0, 'a', 'x'], [1, 'b', 'y'], [2, 'c', 'z'], [3, 'a', 'z'], [4, 'a', 'u'], [5, 'c', 'y']]

df = spark.createDataFrame(test_list, cSchema)

cols = ['id', 'value']

indexers = [StringIndexer(inputCol=column, outputCol=column + '_index', handleInvalid='skip') for column in
            list(set(df.columns) - set(cols))]

pipeline = Pipeline(stages=indexers)
df_cust_indexed = pipeline.fit(df).transform(df)

df_cust_indexed.show()
