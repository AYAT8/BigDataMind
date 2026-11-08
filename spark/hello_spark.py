from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("hello-spark")
    .master("local[*]")
    .getOrCreate()
)

df = spark.range(1_000_000)  # 1 million rows, one column "id"
print("Partitions :", df.rdd.getNumPartitions())
print("Rows       :", df.count())  # action: this is where Spark really works

input("Spark UI at http://localhost:4040 - press Enter to finish")
spark.stop()