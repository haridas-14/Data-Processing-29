
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean
from pyspark.ml.feature import StandardScaler, VectorAssembler

# ---------------------------------------------------------
# 1. Initialize Spark Session
# ---------------------------------------------------------
spark = SparkSession.builder \
    .appName("DataPreprocessing") \
    .getOrCreate()

# ---------------------------------------------------------
# 2. Load Raw Dataset
# ---------------------------------------------------------
# Replace 'dataset.csv' with your file name
df = spark.read.csv("iot_telemetry_data.csv", header=True, inferSchema=True)
print("=== RAW DATA ===")
df.show(5)
df.printSchema()

# ---------------------------------------------------------
# 3. Handle Missing Values
# ---------------------------------------------------------
# Option 1: Drop rows with any null values
# df = df.na.drop()

# Option 2: Fill numeric missing values with mean
numeric_cols = [field.name for field in df.schema.fields if str(field.dataType) in ["IntegerType", "DoubleType"]]

mean_values = df.select([mean(col(c)).alias(c) for c in numeric_cols]).collect()[0].asDict()
df = df.na.fill(mean_values)

print("=== AFTER HANDLING MISSING VALUES ===")
df.show(5)

# ---------------------------------------------------------
# 4. Fix Data Type Inconsistencies
# ---------------------------------------------------------
# Example: casting columns
df = df.withColumn("age", col("age").cast("int")) \
       .withColumn("salary", col("salary").cast("double"))

print("=== AFTER TYPE CASTING ===")
df.printSchema()

# ---------------------------------------------------------
# 5. Remove Duplicates
# ---------------------------------------------------------
df = df.dropDuplicates()
print(f"=== AFTER REMOVING DUPLICATES: {df.count()} records ===")

# ---------------------------------------------------------
# 6. Normalization / Standardization
# ---------------------------------------------------------
# Scale numeric features
assembler = VectorAssembler(inputCols=["age", "salary"], outputCol="features")
assembled = assembler.transform(df)

scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withMean=True, withStd=True)
scaled_df = scaler.fit(assembled).transform(assembled)

print("=== AFTER SCALING ===")
scaled_df.select("features", "scaled_features").show(5, truncate=False)

# ---------------------------------------------------------
# 7. Feature Engineering
# ---------------------------------------------------------
# Create a new feature 'age_group' based on age
final_df = scaled_df.withColumn(
    "age_group",
    when(col("age") < 25, "Young")
    .when((col("age") >= 25) & (col("age") < 50), "Adult")
    .otherwise("Senior")
)

print("=== AFTER FEATURE ENGINEERING ===")
final_df.select("age", "age_group").show(5)

# ---------------------------------------------------------
# 8. Save Cleaned Dataset
# ---------------------------------------------------------
final_df.write.mode("overwrite").csv("cleaned_dataset", header=True)

print("✅ Data preprocessing complete! Cleaned data saved to 'cleaned_dataset/' folder.")

# Stop Spark Session
spark.stop()
