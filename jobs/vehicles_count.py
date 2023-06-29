import pymongo
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark: SparkSession = (
    SparkSession.builder.appName("vehicles_count")
    .config(
        "spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1",
    )
    .config(
        "spark.mongodb.read.connection.uri",
        "mongodb://root:admin@172.17.0.1:27017",
    )
    .config(
        "spark.mongodb.write.connection.uri",
        "mongodb://root:admin@172.17.0.1:27017",
    )
    .config("spark.executor.cores", "1")
    .config("spark.memory.fraction", "0.5")
    .config("spark.executar.memory", "512m")
    .master("spark://localhost:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

connection = pymongo.MongoClient("mongodb://root:admin@172.17.0.1:27017", replicaSet="replicaset", serverSelectionTimeoutMS=1000)

df = (
    spark.readStream.format("mongodb")
    .option("database", "simulator")
    .option("collection", "positions")
    .option("spark.mongodb.change.stream.publish.full.document.only", "true")
    .load()
)

df = df.select(F.approx_count_distinct("plate").alias("count"))

def foreach_batch_function(df, epoch_id):
    data = df.toPandas().to_dict(orient="records")[0]
    connection["analysis"]["vehicles_count"].replace_one({"_id": 1}, data, upsert=True)

query = (
    df.writeStream
    .outputMode("complete")
    .foreachBatch(foreach_batch_function)
    .start()
)

query.awaitTermination()
