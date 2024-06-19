import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.microsoft.azure:azure-storage:8.6.6,org.apache.hadoop:hadoop-azure:3.2.0 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from datetime import datetime
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Define schema for Kafka messages
schema = StructType([
    StructField("number_of_elements", IntegerType()),
    StructField("mean_atomic_mass", FloatType()),
    StructField("wtd_mean_atomic_mass", FloatType()),
    StructField("gmean_atomic_mass", FloatType()),
    StructField("wtd_gmean_atomic_mass", FloatType()),
    StructField("entropy_atomic_mass", FloatType()),
    StructField("wtd_entropy_atomic_mass", FloatType()),
    StructField("range_atomic_mass", FloatType()),
    StructField("wtd_range_atomic_mass", FloatType()),
    StructField("std_atomic_mass", FloatType()),
    StructField("wtd_std_atomic_mass", FloatType()),
    StructField("mean_fie", FloatType()),
    StructField("wtd_mean_fie", FloatType()),
    StructField("gmean_fie", FloatType()),
    StructField("wtd_gmean_fie", FloatType()),
    StructField("entropy_fie", FloatType()),
    StructField("wtd_entropy_fie", FloatType()),
    StructField("range_fie", FloatType()),
    StructField("wtd_range_fie", FloatType()),
    StructField("std_fie", FloatType()),
    StructField("wtd_std_fie", FloatType()),
    StructField("mean_atomic_radius", FloatType()),
    StructField("wtd_mean_atomic_radius", FloatType()),
    StructField("gmean_atomic_radius", FloatType()),
    StructField("wtd_gmean_atomic_radius", FloatType()),
    StructField("entropy_atomic_radius", FloatType()),
    StructField("wtd_entropy_atomic_radius", FloatType()),
    StructField("range_atomic_radius", FloatType()),
    StructField("wtd_range_atomic_radius", FloatType()),
    StructField("std_atomic_radius", FloatType()),
    StructField("wtd_std_atomic_radius", FloatType()),
    StructField("mean_Density", FloatType()),
    StructField("wtd_mean_Density", FloatType()),
    StructField("gmean_Density", FloatType()),
    StructField("wtd_gmean_Density", FloatType()),
    StructField("entropy_Density", FloatType()),
    StructField("wtd_entropy_Density", FloatType()),
    StructField("range_Density", FloatType()),
    StructField("wtd_range_Density", FloatType()),
    StructField("std_Density", FloatType()),
    StructField("wtd_std_Density", FloatType()),
    StructField("mean_ElectronAffinity", FloatType()),
    StructField("wtd_mean_ElectronAffinity", FloatType()),
    StructField("gmean_ElectronAffinity", FloatType()),
    StructField("wtd_gmean_ElectronAffinity", FloatType()),
    StructField("entropy_ElectronAffinity", FloatType()),
    StructField("wtd_entropy_ElectronAffinity", FloatType()),
    StructField("range_ElectronAffinity", FloatType()),
    StructField("wtd_range_ElectronAffinity", FloatType()),
    StructField("std_ElectronAffinity", FloatType()),
    StructField("wtd_std_ElectronAffinity", FloatType()),
    StructField("mean_FusionHeat", FloatType()),
    StructField("wtd_mean_FusionHeat", FloatType()),
    StructField("gmean_FusionHeat", FloatType()),
    StructField("wtd_gmean_FusionHeat", FloatType()),
    StructField("entropy_FusionHeat", FloatType()),
    StructField("wtd_entropy_FusionHeat", FloatType()),
    StructField("range_FusionHeat", FloatType()),
    StructField("wtd_range_FusionHeat", FloatType()),
    StructField("std_FusionHeat", FloatType()),
    StructField("wtd_std_FusionHeat", FloatType()),
    StructField("mean_ThermalConductivity", FloatType()),
    StructField("wtd_mean_ThermalConductivity", FloatType()),
    StructField("gmean_ThermalConductivity", FloatType()),
    StructField("wtd_gmean_ThermalConductivity", FloatType()),
    StructField("entropy_ThermalConductivity", FloatType()),
    StructField("wtd_entropy_ThermalConductivity", FloatType()),
    StructField("range_ThermalConductivity", FloatType()),
    StructField("wtd_range_ThermalConductivity", FloatType()),
    StructField("std_ThermalConductivity", FloatType()),
    StructField("wtd_std_ThermalConductivity", FloatType()),
    StructField("mean_Valence", FloatType()),
    StructField("wtd_mean_Valence", FloatType()),
    StructField("gmean_Valence", FloatType()),
    StructField("wtd_gmean_Valence", FloatType()),
    StructField("entropy_Valence", FloatType()),
    StructField("wtd_entropy_Valence", FloatType()),
    StructField("range_Valence", FloatType()),
    StructField("wtd_range_Valence", FloatType()),
    StructField("std_Valence", FloatType()),
    StructField("wtd_std_Valence", FloatType()),
    StructField("critical_temp", FloatType())
])

# Define Kafka bootstrap servers and topic
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'superconductivity_data'  # Replace with your Kafka topic name

# Define output directory for Parquet files
output_dir = "spark_parquet_op"  # Replace with your desired output directory

def generate_output_path():
    """Generate output path with current date formatted as 'dd_mm_yyyy'."""
    now = datetime.now()
    return f"{output_dir}/{now.strftime('%d_%m_%Y')}"

if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("KafkaToParquet") \
        .getOrCreate()

    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .load() \
    
    
    df = df.selectExpr("CAST(value AS STRING)")
    df.printSchema()
    # Write stream to Parquet
    query = df \
        .coalesce(1) \
        .writeStream \
        .format("parquet") \
        .option("path", generate_output_path()) \
        .option("checkpointLocation", "chkpnt") \
        .outputMode("append") \
        .start()

    # Wait for termination
    query.awaitTermination()
    spark.stop()
