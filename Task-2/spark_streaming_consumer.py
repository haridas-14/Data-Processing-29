# spark_streaming_consumer.py
# Usage: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 spark_streaming_consumer.py
import json, time, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType, StringType
from sklearn.linear_model import SGDRegressor
import joblib
import numpy as np

MODEL_PATH = "incremental_model.joblib"

def ensure_model():
    if os.path.exists(MODEL_PATH):
        model = joblib.load(MODEL_PATH)
    else:
        # create initial model with 1 feature placeholder and call partial_fit once with dummy data
        model = SGDRegressor()
        X_dummy = np.array([[0.0]])
        y_dummy = np.array([0.0])
        model.partial_fit(X_dummy, y_dummy)
        joblib.dump(model, MODEL_PATH)
    return model

def foreach_batch_function(df, epoch_id):
    # This runs on the driver for each micro-batch
    # collect features and update incremental model
    rows = df.select("temp", "humidity", "temp_humidity_ratio").collect()
    if not rows:
        return
    X = np.array([[r['temp'], r['humidity'], r['temp_humidity_ratio']] for r in rows])
    # As example, we'll predict 'temp' from other features; so target is temp (could be a real label)
    y = X[:, 0]  # here just use temp as target for demo
    # reduce features for model shape (use humidity & ratio)
    X_model = X[:, 1:]  # shape (n_samples,2)
    model = ensure_model()
    model.partial_fit(X_model, y)
    joblib.dump(model, MODEL_PATH)
    print(f"[{time.ctime()}] updated model on {len(rows)} samples")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("id", IntegerType()),
        StructField("ts", LongType()),
        StructField("temp", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("status", StringType())
    ])

    kafkaDF = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "sensor-data")
        .option("startingOffsets", "earliest")
        .load()
    )

    jsonDF = kafkaDF.selectExpr("CAST(value AS STRING) as json_str")
    parsed = jsonDF.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

    # Derived column
    parsed = parsed.withColumn("temp_humidity_ratio",
                               col("temp") / col("humidity"))

    # compute rolling average over a 10-second window by id
    agg = (
        parsed
        .withWatermark("ts", "30 seconds")
        .groupBy(window(col("ts")/1000, "10 seconds"), col("id"))
        .agg(
            avg("temp").alias("avg_temp"),
            avg("humidity").alias("avg_humidity")
        )
    )

    # write aggregates to console (for debug) and also call foreachBatch to update model with raw rows
    query = (
        parsed.writeStream
        .format("console")
        .option("truncate", False)
        .foreachBatch(foreach_batch_function)
        .start()
    )

    query.awaitTermination()
