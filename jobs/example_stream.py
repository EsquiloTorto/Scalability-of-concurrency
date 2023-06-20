from pyspark.sql import SparkSession, DataFrame


spark: SparkSession = (
    SparkSession.builder.appName("example_stream")
    .master("spark://spark-master:7077")
    .config(
        "spark.mongodb.read.connection.uri",
        "mongodb://root:admin@mongodb:27017",
    )
    .config(
        "spark.mongodb.write.connection.uri",
        "mongodb://root:admin@mongodb:27017",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark session created")

df = (
    spark.readStream.format("mongodb")
    .option("database", "simulator")
    .option("collection", "positions")
    .load()
)

df = df.selectExpr("count(*) as count")

query = (
    df.writeStream.format("console")
    .trigger(processingTime="2 second")
    .outputMode("complete")
)

query.start().awaitTermination()
