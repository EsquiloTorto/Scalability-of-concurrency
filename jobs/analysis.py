from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from math import exp

max_speed = 10.0

# Define o esquema do dos dados lidos do MongoDB
readSchema = StructType([
    StructField("_id", StringType(), False),
    StructField("plate", StringType(), False),
    StructField("highway", StringType(), False),
    StructField("dist", IntegerType(), False),
    StructField("lane", IntegerType(), False),
    StructField("timestamp", DoubleType(), False),
])

movementStruct = StructType([
    StructField("speed", DoubleType(), False),
    StructField("acceleration", DoubleType(), False),
    StructField("risk", DoubleType(), False),
    # StructField("ticket_count", IntegerType(), False),
    # StructField("ticket", BooleanType(), True),
])

def sigmoid(x):
    return 1 / (1 + exp(-x))

@F.udf(returnType=movementStruct)
def compute_movement(positions):
    if len(positions) <= 1:
        return float("nan"), float("nan"), float("nan")
    else:
        speed = (positions[-1].dist - positions[-2].dist) / (positions[-1].timestamp - positions[-2].timestamp)
        if len(positions) <= 2:
            acceleration = float("nan")
            risk = float("nan")
        else:
            # Delta speed / delta time
            acceleration = (
                ((positions[-2].dist - positions[-3].dist)
                / (positions[-2].timestamp - positions[-3].timestamp)
                - speed) / (positions[-1].timestamp - positions[-2].timestamp)
            )
            risk = sigmoid(3.0 * (speed * (abs(acceleration) + 1)) / max_speed - 5.0)
    return speed, acceleration, risk


spark: SparkSession = (
    SparkSession.builder.appName("ETL")
    .master("spark://spark-master:7077")
    .config(
        "spark.mongodb.read.connection.uri",
        "mongodb://root:admin@mongodb:27017",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark session created")


df = (
    spark.read.format("mongodb")
    .option("database", "simulator")
    .option("collection", "positions")
    .schema(readSchema)
    .load()
).withColumn(
    "position", F.struct("timestamp", "dist", "lane")
).drop(
    "timestamp", "dist", "lane"
)

# Agrupa as posições de cada veículo e ordena pelo instante de tempo em que ocorreram para
# prevenir indeterminismo dos processamento em paralelo. Em seguida, calcula o número de
# rodovias distintas, velocidade, aceleção e risco para cada veículo
priority = df.groupBy("plate").agg(
    F.sort_array(F.collect_list("position")).alias("positions"),
    F.approx_count_distinct("highway").alias("highway_count"),
).withColumn(
    "movement", compute_movement("positions"),
)

result = priority.select(
    F.count("*").alias("vehicle_count"),
    F.collect_list(F.when(F.col("movement.risk") > 0.5, F.col("plate"))).alias("risky_vehicles"),
    F.collect_list(F.when(F.col("movement.speed") > max_speed, F.col("plate"))).alias("speeding_vehicles"),
).withColumns({
    "risky_count": F.size("risky_vehicles"),
    "speeding_count": F.size("speeding_vehicles"),
})

result = (result
    .join(
        df.select(F.approx_count_distinct("highway").alias("total_highways"))
    )
    .join(
        priority
            .select("highway_count")
            .orderBy("highway_count", ascending=False).limit(100)
            .select(F.collect_list("highway_count").alias("top100"))
    )
)

# Escreve o resultado das agregações no MongoDB
query = (
    result.write.format("mongodb")
    .option(
        "spark.mongodb.write.connection.uri",
        "mongodb://root:admin@mongodb:27017",
    )
    .option("database", "analysis")
    .option("collection", "aggregations")
    .option("checkpointLocation", "/tmp/checkpoint/")
    .mode("append")
)

query.save()
