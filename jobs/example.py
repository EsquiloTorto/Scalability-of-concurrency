from pyspark.sql import SparkSession
import random

spark: SparkSession = (
    SparkSession.builder.appName("example")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

NUM_SAMPLES = 100000

def inside(_):
    x, y = random.random(), random.random()
    return x * x + y * y < 1

count = spark.sparkContext.parallelize(range(0, NUM_SAMPLES)).filter(inside).count()
print(f"Pi is roughly {(4.0 * count / NUM_SAMPLES)}")
