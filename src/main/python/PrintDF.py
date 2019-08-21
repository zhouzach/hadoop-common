from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("member_statistics_6month") \
    .config('spark.sql.hive.caseSensitiveInferenceMode', 'INFER_ONLY') \
    .config('spark.dynamicAllocation.enabled', 'false') \
    .config('spark.lineage.enabled', 'false') \
    .enableHiveSupport() \
    .getOrCreate()

labels=spark.sql("""
SELECT * FROM db1.t1 LIMIT 100
""")

labels.show(10)

print(labels.collect())
