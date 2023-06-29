from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from sys import maxsize


max_speed = 10.0

# Define o esquema dos dados lidos do MongoDB
readSchema = StructType([
    StructField("_id", StringType(), False),
    StructField("plate", StringType(), False),
    StructField("highway", StringType(), False),
    StructField("dist", IntegerType(), False),
    StructField("lane", IntegerType(), False),
    StructField("unix_time", DoubleType(), False)
])

def time_to_str(T):
    if isinstance(T, str):
        return T
    if T < 60:
        return f"{T} seconds"
    elif T < 3600:
        return f"{T // 60} minutes"
    elif T < 86400:
        return f"{T // 3600} hours"
    raise ValueError("Pega leve, cara")

spark: SparkSession = (
    SparkSession.builder.appName("ETL")
    .master("spark://spark-master:7077")
    .config(
        "spark.mongodb.read.connection.uri",
        "mongodb://root:admin@mongodb:27017",
    )
    .getOrCreate()
)

spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
spark.sparkContext.setLogLevel("WARN")

print("Spark session created")



# ---------- Parâmetros ----------
max_speed = 3.0
# Tempo para o qual o deslocamento é simulado, pouco mais que o tempo de reação humano
risk_time = 0.5
T = 30

half_T = time_to_str(30 // 2)
T = time_to_str(T)
# Equivalente a multiplicar por 2 para evitar refazer toda vez na fórmula
risk_time_squared = risk_time**2 * 0.5
window = F.window("timestamp", T, half_T)



df = (
    spark.readStream.format("mongodb")
    .option("database", "simulator")
    .option("collection", "positions")
    .option("spark.mongodb.change.stream.publish.full.document.only", "true")
    .schema(readSchema)
    .load()
    .withColumn(
        "timestamp", F.timestamp_seconds(F.col("unix_time"))
    )
    .withWatermark("timestamp", T)
)

priority = (
    df
    # Coleta o timestamp, a posição na via e o número da via em um array de struct
    .groupBy("plate", "highway", window).agg(
        F.collect_list("unix_time").alias("unix_time"),
        F.collect_list("dist").alias("dist"),
        F.collect_list("lane").alias("lane")
    ).withColumn(
        # Ordena o array de struct por tempo
        "position", F.array_sort(F.arrays_zip("unix_time", "dist", "lane"))
    ).withColumn(
        # Calcula o tempo entre as duas últimas posições (None se não houver duas posições)
        "delta_time", (
            F.element_at("position", -1).getField("unix_time")
            - F.element_at("position", -2).getField("unix_time")
        )
    )
    .withColumns({
        # v = ΔS / Δt
        "speed": (
            F.element_at("position", -1).getField("dist")
            - F.element_at("position", -2).getField("dist")) / F.col("delta_time"),
        "prev_speed":
            (F.element_at("position", -2).getField("dist")
            - F.element_at("position", -3).getField("dist"))
            / (F.element_at("position", -2).getField("unix_time")
                - F.element_at("position", -3).getField("unix_time"))
    })
    # Obtém uma coluna de booleanos indicando se o veículo está acima da velocidade máxima,
    # uma coluna com a aceleração e uma coluna com a posição futura do veículo
    .withColumns({
        "is_speeding": F.col("speed") > max_speed,
        "acceleration": (F.col("speed") - F.col("prev_speed")) / F.col("delta_time"),
        # S = S_0 + v_0 * t + 1/2 * a * t^2, movimento retilíneo uniformemente variado
        "future_position":
            F.element_at("position", -1).getField("dist")
            + F.col("speed") * risk_time + F.col("acceleration") * risk_time_squared
    }).drop("window", "unix_time", "dist", "lane")
)

# w = Window.partitionBy("position.lane").orderBy("future_position")
# Agrupa os veículos (que já estão agrupados por rodovia) por via e os dispõe em ordem de posição futura.
# Se a estimativa de posição futura do veículo atual for à frente do próximo, considera-o com risco de colisão.
result = (
    priority
    # .withColumn(
    #     # Se não há próxima posição, usa maxsize, que sempre fará retornar False
    #     "is_risky", F.col("future_position") >= F.lead("future_position", 1, maxsize).over(w)
    # )
    .select(
        F.count("*").alias("vehicle_count"),
        F.collect_list(F.when(F.col("is_speeding"),
            F.struct(
                F.col("plate"),
                F.col("speed"),
                # F.col("is_risky")
            )
        )).alias("speeding_vehicles"),
        # F.collect_list(F.when(F.col("is_risky"),
        #     F.struct(
        #         F.col("plate"),
        #         F.col("speed")
        #     )
        # )).alias("risky_vehicles")
    ).withColumns({
        # "risky_count": F.size("risky_vehicles"),
        "speeding_count": F.size("speeding_vehicles"),
    })
)

# priority_query = (
#     priority.writeStream.format("mongodb")
#     .trigger(processingTime="1 second")
#     .outputMode("append")
#     .option(
#         "spark.mongodb.output.uri",
#         "mongodb://root:admin@localhost:27017/analysis.aggregations?authSource=admin"
#     )
#     .option("database", "analysis")
#     .option("collection", "aggregations")
#     .option("checkpointLocation", "/tmp/checkpoint/")
# ).start()

query = (
    result.writeStream.format("console")
    .trigger(processingTime="1 second")
    .outputMode("complete")
).start()
