# Databricks notebook source
""" DATA LAKE BRONZE LANDING ZONE FROM SENSOR STREAM PIPELINE"""

# COMMAND ----------

# %pip install great_expectations


# %restart_python


# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------

production_src = "/Volumes/ong_streamworkspace_37859/default/ong_sensorstream/output"
destinationlakeCheckpoints = "/Volumes/ong_streamworkspace_37859/default/checkpoints"
outlake = "outlake.landing"

# COMMAND ----------

src_list = ['production-daily-data', 'reservoir', 'equipment-events', 'wellbore', 'facility-telemetry', 'well-telemetry']

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# SCHEMA-BASED SOURCE INGESTION TEST - SEPARATE INGESTION AND LOADING INTO DELTALAKE

# from pyspark.sql.types import *
# from pyspark.sql.functions import *
# import great_expectations as gx


# """
# This notebook cell mocks the final standard for reading in data from streaming sensor data in remote production environments

# """

# # Create a stream that reads data from the folder, using producer context data contract schema
# inputPath = f'{production_src}/well-telemetry'
# OnGWell_telemetryProdContract = StructType([
# StructField("device", StringType(), False),
# StructField("status", StringType(), False)
# ])
# iotstream = spark.readStream.schema(OnGWell_telemetryProdContract).option("maxFilesPerTrigger", 1).parquet(inputPath)
# print("Source stream created...")


# out = f"{destinationlake}/well-telemetry"
# outcheckpoint = f"{destinationlake}/well-telemetry/checkpoint"
# iotstream.writeStream.format("delta").option("checkpointLocation", outcheckpoint).start(out)
# print("Streaming to delta sink)


# COMMAND ----------

# SCHEMA-BASED SOURCE INGESTION PROTOTYPE - COMBINED INGESTION AND LOADING INTO DELTALAKE

# from pyspark.sql.types import *
# from pyspark.sql.functions import *
# import great_expectations as gx


# """
# This notebook cell mocks the final standard for reading in data from streaming sensor data in remote production environments

# """

# # Create a stream that reads data from the folder, using producer context data contract schema
# inputPath = f'{production_src}/well-telemetry'
# outputPath = f"{destinationlake}/well-telemetry"
# outputCheckpoint = f"{destinationlake}/well-telemetry/checkpoint"


# OnGWell_telemetryProdContract = StructType([
# StructField("device", StringType(), False),
# StructField("status", StringType(), False)
# ])


# display(spark.readStream.schema(OnGWell_telemetryProdContract).option("maxFilesPerTrigger", 1).parquet(inputPath).writeStream.format("delta").option("checkpointLocation", outputCheckpoint).start(outputPath))
# print("Source stream created...")

# print("Streaming to delta sink)


# COMMAND ----------

# DBTITLE 1,Cell 2
# CHECKPOINT-BASED SOURCE INGESTION TEST - SEPARATE INGESTION AND LOADING CHECKPOINTS

# # Now reference the volume path
# file_path = f'{production_src}/well-telemetry'
# src_checkpointpath = f'{production_src}/well-telemetry/_checkpoint'

# raw_df = (spark.readStream
#     .format("cloudFiles")
#     .option("cloudFiles.format", "parquet")
#     .option("cloudFiles.schemaLocation", src_checkpointpath)
#     .load(file_path)
# )
# print("Retrieved Well Data From Source")
# display(raw_df, checkpointLocation = src_checkpointpath)

# delta_stream_table_path = f'{destinationlake}/ong-welldata'
# dest_checkpointpath = f'{destinationlake}/ong-welldata/checkpoint'
# raw_df.writeStream \
#     .trigger(availableNow=True).format("delta").option("checkpointLocation", dest_checkpointpath).start(delta_stream_table_path)
# print("Streaming to delta sink...")


# COMMAND ----------

# VOLUME-BASED SOURCE INGESTION PROTOTYPE - INGESTION INTO DATAFRAME, VIEWING AND LOADING USING CATALOG VOLUMES

# # Create external volume using the analytics_data_stream external location
# spark.sql("""
# CREATE EXTERNAL VOLUME IF NOT EXISTS ong_streamworkspace_37859.default.analytics_data_stream
# LOCATION 'abfss://analyticscontainer@analyticsstorage37859.dfs.core.windows.net/'
# """)

# # Now reference the volume path
# file_path = "/Volumes/ong_streamworkspace_37859/default/analytics_data_stream/output/"
# checkpoint_path = "/Volumes/ong_streamworkspace_37859/default/analytics_data_stream/_checkpoint"

# # checkpoint_path = "abfss://dev-bucket@<storage-account>.dfs.core.windows.net/_checkpoint/dev_table"

# raw_df = (spark.readStream
#   .format("cloudFiles")
#   .option("cloudFiles.format", "parquet")
#   .option("cloudFiles.schema", parquetSchema)
#   .load("analytics_data_stream/json-data"))


# # Create catalog and schema if they don't exist
# spark.sql("CREATE CATALOG IF NOT EXISTS dev_catalog")
# spark.sql("CREATE SCHEMA IF NOT EXISTS analytics_data_stream.dev_database")


# display(raw_df.writeStream
#   .option("checkpointLocation", checkpoint_path)
#   .trigger(availableNow=True)
#   .toTable("analytics_data_stream.dev_database.dev_table"))




# COMMAND ----------

# DAILY-AGGREGATE-EVENTS LANDINGZONE SINK
dailyprod_file_path = f'{production_src}/{src_list[0]}'
dailyprod_src_checkpointpath = f'{dailyprod_file_path}/_checkpoint'
dailyprod_dest_checkpointpath = f'{destinationlakeCheckpoints}/landing/dailyprod'
DPoutputPath = f"{outlake}.ong_dailyproddata"




# Write stream to table
display(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", dailyprod_src_checkpointpath)
    .load(dailyprod_file_path).writeStream
  .option("checkpointLocation", dailyprod_dest_checkpointpath)
  .trigger(availableNow=True)
  .toTable(DPoutputPath))

# COMMAND ----------

# RESERVOIR-EVENTS LANDINGZONE SINK
reservoir_file_path = f'{production_src}/{src_list[1]}'
reservoir_src_checkpointpath = f'{reservoir_file_path}/_checkpoint'
reservoir_dest_checkpointpath = f'{destinationlakeCheckpoints}/landing/reservoir'
RoutputPath = f"{outlake}.ong_reservoirdata"




# Write stream to table
display(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", reservoir_src_checkpointpath)
    .load(reservoir_file_path).writeStream
  .option("checkpointLocation", reservoir_dest_checkpointpath)
  .trigger(availableNow=True)
  .toTable(RoutputPath))

# COMMAND ----------

# EQUIPMENTS-EVENTS LANDINGZONE SINK
equipments_file_path = f'{production_src}/{src_list[2]}'
equipments_src_checkpointpath = f'{equipments_file_path}/_checkpoint'
equipments_dest_checkpointpath = f'{destinationlakeCheckpoints}/landing/equipment'
EoutputPath = f"{outlake}.ong_equipmentdata"




# Write stream to table
display(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", equipments_src_checkpointpath)
    .load(equipments_file_path).writeStream
  .option("checkpointLocation", equipments_dest_checkpointpath)
  .trigger(availableNow=True)
  .toTable(EoutputPath))

# COMMAND ----------

# WELLBORE-EVENTS LANDINGZONE SINK
wellbore_file_path = f'{production_src}/{src_list[3]}'
wellbore_src_checkpointpath = f'{wellbore_file_path}/_checkpoint'
wellbore_dest_checkpointpath = f'{destinationlakeCheckpoints}/landing/wellbore'
WBoutputPath = f"{outlake}.ong_wellboredata"




# Write stream to table
display(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", wellbore_src_checkpointpath)
    .load(wellbore_file_path).writeStream
  .option("checkpointLocation", wellbore_dest_checkpointpath)
  .trigger(availableNow=True)
  .toTable(WBoutputPath))

# COMMAND ----------

# FACILITY-EVENTS LANDINGZONE SINK
facility_file_path = f'{production_src}/{src_list[4]}'
facility_src_checkpointpath = f'{facility_file_path}/_checkpoint'
facility_dest_checkpointpath = f'{destinationlakeCheckpoints}/landing/facility'
FoutputPath = f"{outlake}.ong_facilitydata"




# Write stream to table
display(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", facility_src_checkpointpath)
    .load(facility_file_path).writeStream
  .option("checkpointLocation", facility_dest_checkpointpath)
  .trigger(availableNow=True)
  .toTable(FoutputPath))

# COMMAND ----------

# WELL-TELEMETRY LANDINGZONE SINK
well_file_path = f'{production_src}/{src_list[5]}'
well_src_checkpointpath = f'{well_file_path}/_checkpoint'
well_dest_checkpointpath = f'{destinationlake}/landing/welldata'
WoutputPath = f"{outlake}.ong_welldata"



# Write stream to table
display(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", well_src_checkpointpath)
    .load(well_file_path).writeStream
  .option("checkpointLocation", well_dest_checkpointpath)
  .trigger(availableNow=True)
  .toTable(WoutputPath))

# COMMAND ----------

print("LANDING ZONE SINKING COMPLETED")

# COMMAND ----------


# for src in source_list:
#     if src == 'production-daily-data':
#         daily_aggregate_dump(production_src, src, destinationlakeCheckpoints, outlake)
#     elif src == 'reservoir':
#         reservoir_events_dump(production_src, src, destinationlakeCheckpoints, outlake)
#     elif src == 'equipment-events':
#         equipment_events_dump(production_src, src, destinationlakeCheckpoints, outlake)
#     elif src == 'wellbore':
#         wellbore_events_dump(production_src, src, destinationlakeCheckpoints, outlake)
#     elif src == 'facility':
#         facility_telemetry_dump(production_src, src, destinationlakeCheckpoints, outlake)
#     elif src == 'well':
#         well_telemetry_dump(production_src, src, destinationlakeCheckpoints, outlake)

