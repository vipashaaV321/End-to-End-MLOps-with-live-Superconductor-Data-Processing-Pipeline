from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType,FloatType, DoubleType, IntegerType
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.microsoft.azure:azure-storage:8.6.6,org.apache.hadoop:hadoop-azure:3.2.0 pyspark-shell'

storage_account_name = 'vipasha'
storage_account_key = 'oTRZNpvHO2pKAUlRctA2cgtq1eJAX3lDTjJD4pNdzkEy+3Ib2x9DsCrWEJ/+HG3jmax7jz/cuqmW+AStV6AU0A=='
storage_container_name = 'etl-storage-container'

# Define output path and filename
output_path = f"wasbs://{storage_container_name}@{storage_account_name}.blob.core.windows.net/bronze/"
current_date = datetime.now().strftime("%d_%m_%Y")
output_file = f"{current_date}_vip_processed.parquet"

# Initialize Spark session
spark = SparkSession.builder.appName("Kakfa_Project_Topic").getOrCreate()
# Set Azure Storage account key
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key)
 
# Set log level to minimize verbosity
spark.sparkContext.setLogLevel("WARN")

# Define the schema of the Kafka messages (example schema)
schema = StructType([
        StructField("number_of_elements", FloatType(), True),
        StructField("mean_atomic_mass", DoubleType(), True),
        StructField("wtd_mean_atomic_mass", DoubleType(), True),
        StructField("gmean_atomic_mass", DoubleType(), True),
        StructField("wtd_gmean_atomic_mass", DoubleType(), True),
        StructField("entropy_atomic_mass", DoubleType(), True),
        StructField("wtd_entropy_atomic_mass", DoubleType(), True),
        StructField("range_atomic_mass", DoubleType(), True),
        StructField("wtd_range_atomic_mass", DoubleType(), True),
        StructField("std_atomic_mass", DoubleType(), True),
        StructField("wtd_std_atomic_mass", DoubleType(), True),
        StructField("mean_fie", DoubleType(), True),
        StructField("wtd_mean_fie", DoubleType(), True),
        StructField("gmean_fie", DoubleType(), True),
        StructField("wtd_gmean_fie", DoubleType(), True),
        StructField("entropy_fie", DoubleType(), True),
        StructField("wtd_entropy_fie", DoubleType(), True),
        StructField("range_fie", DoubleType(), True),
        StructField("wtd_range_fie", DoubleType(), True),
        StructField("std_fie", DoubleType(), True),
        StructField("wtd_std_fie", DoubleType(), True),
        StructField("mean_atomic_radius", DoubleType(), True),
        StructField("wtd_mean_atomic_radius", DoubleType(), True),
        StructField("gmean_atomic_radius", DoubleType(), True),
        StructField("wtd_gmean_atomic_radius", DoubleType(), True),
        StructField("entropy_atomic_radius", DoubleType(), True),
        StructField("wtd_entropy_atomic_radius", DoubleType(), True),
        StructField("range_atomic_radius", DoubleType(), True),
        StructField("wtd_range_atomic_radius", DoubleType(), True),
        StructField("std_atomic_radius", DoubleType(), True),
        StructField("wtd_std_atomic_radius", DoubleType(), True),
        StructField("mean_Density", DoubleType(), True),
        StructField("wtd_mean_Density", DoubleType(), True),
        StructField("gmean_Density", DoubleType(), True),
        StructField("wtd_gmean_Density", DoubleType(), True),
        StructField("entropy_Density", DoubleType(), True),
        StructField("wtd_entropy_Density", DoubleType(), True),
        StructField("range_Density", DoubleType(), True),
        StructField("wtd_range_Density", DoubleType(), True),
        StructField("std_Density", DoubleType(), True),
        StructField("wtd_std_Density", DoubleType(), True),
        StructField("mean_ElectronAffinity", DoubleType(), True),
        StructField("wtd_mean_ElectronAffinity", DoubleType(), True),
        StructField("gmean_ElectronAffinity", DoubleType(), True),
        StructField("wtd_gmean_ElectronAffinity", DoubleType(), True),
        StructField("entropy_ElectronAffinity", DoubleType(), True),
        StructField("wtd_entropy_ElectronAffinity", DoubleType(), True),
        StructField("range_ElectronAffinity", DoubleType(), True),
        StructField("wtd_range_ElectronAffinity", DoubleType(), True),
        StructField("std_ElectronAffinity", DoubleType(), True),
        StructField("wtd_std_ElectronAffinity", DoubleType(), True),
        StructField("mean_FusionHeat", DoubleType(), True),
        StructField("wtd_mean_FusionHeat", DoubleType(), True),
        StructField("gmean_FusionHeat", DoubleType(), True),
        StructField("wtd_gmean_FusionHeat", DoubleType(), True),
        StructField("entropy_FusionHeat", DoubleType(), True),
        StructField("wtd_entropy_FusionHeat", DoubleType(), True),
        StructField("range_FusionHeat", DoubleType(), True),
        StructField("wtd_range_FusionHeat", DoubleType(), True),
        StructField("std_FusionHeat", DoubleType(), True),
        StructField("wtd_std_FusionHeat", DoubleType(), True),
        StructField("mean_ThermalConductivity", DoubleType(), True),
        StructField("wtd_mean_ThermalConductivity", DoubleType(), True),
        StructField("gmean_ThermalConductivity", DoubleType(), True),
        StructField("wtd_gmean_ThermalConductivity", DoubleType(), True),
        StructField("entropy_ThermalConductivity", DoubleType(), True),
        StructField("wtd_entropy_ThermalConductivity", DoubleType(), True),
        StructField("range_ThermalConductivity", DoubleType(), True),
        StructField("wtd_range_ThermalConductivity", DoubleType(), True),
        StructField("std_ThermalConductivity", DoubleType(), True),
        StructField("wtd_std_ThermalConductivity", DoubleType(), True),
        StructField("mean_Valence", DoubleType(), True),
        StructField("wtd_mean_Valence", DoubleType(), True),
        StructField("gmean_Valence", DoubleType(), True),
        StructField("wtd_gmean_Valence", DoubleType(), True),
        StructField("entropy_Valence", DoubleType(), True),
        StructField("wtd_entropy_Valence", DoubleType(), True),
        StructField("range_Valence", DoubleType(), True),
        StructField("wtd_range_Valence", DoubleType(), True),
        StructField("std_Valence", DoubleType(), True),
        StructField("wtd_std_Valence", DoubleType(), True),
        StructField("critical_temp", DoubleType(), True)
])

# Read streaming data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "superconductivity_data") \
    .option("startingOffsets", "earliest")\
    .load()
 
# Select the value column and cast it to string
value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")

# Parse JSON data
parsed_df = value_df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")

print(parsed_df)

# Write the processed data to a local parquet file
query = parsed_df\
    .coalesce(1) \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path + output_file) \
    .option("checkpointLocation", "chk-point-dir") \
    .trigger(processingTime='780 seconds') \
    .start()
 
# Await termination
query.awaitTermination()
