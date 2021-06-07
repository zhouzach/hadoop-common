from pyspark.sql import SparkSession

org.rabbit.spark = SparkSession.builder \
    .appName("member_statistics_6month") \
    .config('org.rabbit.spark.sql.hive.caseSensitiveInferenceMode', 'INFER_ONLY') \
    .config('org.rabbit.spark.dynamicAllocation.enabled', 'false') \
    .config('org.rabbit.spark.lineage.enabled', 'false') \
    .enableHiveSupport() \
    .getOrCreate()

labels=org.rabbit.spark.sql("""
SELECT * FROM db1.t1 LIMIT 100
""")

labels.show(10)

print(labels.collect())
