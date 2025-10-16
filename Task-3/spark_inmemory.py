# spark_inmemory.py
# Usage: spark-submit spark_inmemory.py <input_parquet>
import sys
import time
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import avg

def timed(f):
    s = time.time()
    res = f()
    e = time.time()
    print(f"Time: {e-s:.3f}s")
    return res

def main(path):
    spark = SparkSession.builder.appName("InMemoryProcessing").getOrCreate()
    df = spark.read.parquet(path)

    # baseline: query without caching
    print("Baseline query without caching:")
    timed(lambda: df.select(avg("temp")).collect())

    # persist in memory
    print("Persisting dataframe to memory")
    df.persist(StorageLevel.MEMORY_ONLY)

    # warmed query
    print("Query after caching:")
    timed(lambda: df.select(avg("temp")).collect())

    # run some heavier in-memory operations (groupBy)
    print("GroupBy operation:")
    timed(lambda: df.groupBy("status").count().collect())

    df.unpersist()
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit spark_inmemory.py <input_parquet>")
        sys.exit(1)
    main(sys.argv[1])
