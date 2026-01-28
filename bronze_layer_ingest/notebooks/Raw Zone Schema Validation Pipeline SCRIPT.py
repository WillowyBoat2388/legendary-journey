# Databricks notebook source
# MAGIC %pip install great_expectations
# MAGIC
# MAGIC
# MAGIC %restart_python
# MAGIC

# COMMAND ----------

"""DATA LAKE BRONZE RAW ZONE WITH SCHEMA VALIDATION FROM LANDING ZONE"""

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

sourcezone = "outlake.landing"

# COMMAND ----------

src_list = ['ong_dailyproddata', 'ong_reservoirdata', 'ong_equipmentdata', 'ong_wellboredata', 'ong_facilitydata', 'ong_welldata']

# COMMAND ----------

src_tables = [f'{sourcezone}.{table}' for table in src_list]

# COMMAND ----------

# PRODUCER DATA CONTRACT
producer_well_telemetry_schema = {
    "client_id": {"size": None, "dtype": "StringType", "unique": False, "nullable": False},
    "well_id": {"size": None, "dtype": "StringType", "unique": False, "nullable": False},
    "sensor_id": {"size": None, "dtype": "StringType", "unique": True, "nullable": False},
    "sensor_type": {"size": None, "dtype": "StringType", "unique": False, "nullable": False},
    "location": {"size": None, "dtype": "StringType", "unique": False, "nullable": False},
    "timestamp": {"size": None, "dtype": "TimestampType", "unique": True, "nullable": False}, 
    "value": {"size": 255, "dtype": "DoubleType", "unique": True, "nullable": False},
    "unit": {"size": None, "dtype": "StringType", "unique": False, "nullable": False},
    "quality": {"size": None, "dtype": "StringType", "unique": False},
    "status": {"size": None, "dtype": "StringType", "unique": True, "nullable": False},
    "partition_identity": {"size": None, "dtype": "StringType", "unique": True, "nullable": False},
    "_rescued_data": {"size": None, "dtype": "StringType", "unique": True, "nullable": False}
}

wellt_expectations = {"timestamp_valid": "timestamp IS NOT NULL", "client_id_valid": "client_id IS NOT NULL", "well_id_valid": "well_id IS NOT NULL", "sensor_id_valid": "sensor_id IS NOT NULL"}

CLIENTS = [
    {
        'client_id': 'indie_ep_001',
        'operator_name': 'Permian Resources LLC',
        'basin': 'Permian',
        'asset_count': {'wells': 15, 'facilities': 3, 'sensors_per_well': 8}
    },
    {
        'client_id': 'indie_ep_002', 
        'operator_name': 'Eagle Ford Energy Partners',
        'basin': 'Eagle_Ford',
        'asset_count': {'wells': 8, 'facilities': 2, 'sensors_per_well': 6}
    },
    {
        'client_id': 'indie_ep_003',
        'operator_name': 'Bakken Production Co',
        'basin': 'Bakken',
        'asset_count': {'wells': 22, 'facilities': 4, 'sensors_per_well': 10}
    },
    {
        'client_id': 'indie_ep_004',
        'operator_name': 'DJ Basin Operators',
        'basin': 'DJ_Basin',
        'asset_count': {'wells': 12, 'facilities': 2, 'sensors_per_well': 7}
    },
    {
        'client_id': 'indie_ep_005',
        'operator_name': 'Anadarko Field Services',
        'basin': 'Anadarko',
        'asset_count': {'wells': 18, 'facilities': 3, 'sensors_per_well': 9}
    }
  ]







# COMMAND ----------

# DBTITLE 1,Untitled


from pyspark import pipelines as dp

@dp.table
@dp.expect_all_or_fail(valid_pages)
def dailyprod_events():
    # Read individual stream endpoints into dataframes
    return spark.readStream.table(src_tables[0])


@dp.table
@dp.expect_all_or_fail(valid_pages)
def reservoir_events():
    return spark.readStream.table(src_tables[1])


@dp.table
@dp.expect_all_or_fail(valid_pages)
def equipment_events():
    return spark.readStream.table(src_tables[2])


@dp.table
@dp.expect_all_or_fail(valid_pages)
def wellbore_events():

    return spark.readStream.table(src_tables[3])


@dp.table
@dp.expect_all_or_fail(valid_pages)
def facility_events():

    return spark.readStream.table(src_tables[4])


@dp.table
@dp.expect_all_or_fail(wellt_expectations)
def well_telemetry():
    return spark.readStream.table(src_tables[5])





# COMMAND ----------



# COMMAND ----------




# COMMAND ----------

# MAGIC %sql
# MAGIC -- # CREATE CATALOG IF NOT EXISTS outlake
# MAGIC -- # MANAGED LOCATION 'abfss://analyticscontainer@analyticsstorage37859.dfs.core.windows.net/deltalake'
# MAGIC -- # COMMENT 'Catalog for OnG Lakeflow medallion pipelines';
# MAGIC -- # CREATE SCHEMA IF NOT EXISTS outlake.landing
# MAGIC -- #   COMMENT 'Raw/bronze zone schema for Lakeflow pipeline';
# MAGIC -- # CREATE SCHEMA IF NOT EXISTS outlake.raw
# MAGIC -- #   COMMENT 'Raw/bronze zone schema for Lakeflow pipeline';
# MAGIC
# MAGIC
# MAGIC -- # CREATE EXTERNAL VOLUME IF NOT EXISTS ong_streamworkspace_37859.default.great_expectations
# MAGIC -- # LOCATION 'abfss://analyticscontainer@analyticsstorage37859.dfs.core.windows.net/great_expectations'
# MAGIC
# MAGIC -- # CREATE EXTERNAL VOLUME IF NOT EXISTS ong_streamworkspace_37859.default.ong_sensorstream
# MAGIC -- # LOCATION 'abfss://analyticscontainer@analyticsstorage37859.dfs.core.windows.net/analytics'
# MAGIC
# MAGIC -- # CREATE EXTERNAL VOLUME IF NOT EXISTS ong_streamworkspace_37859.default.checkpoints
# MAGIC -- # LOCATION 'abfss://analyticscontainer@analyticsstorage37859.dfs.core.windows.net/deltalake/checkpoints'
# MAGIC
# MAGIC

# COMMAND ----------

# # SOURCE DATA VALIDATION/QUALITY CHECKS DEFINITION - PRODUCER DATA CONTRACT


# def validate_with_gx(
#     df: DataFrame,
#     schema: dict,
#     expected_row_count: int = None,
#     check_ordered_columns: bool = True,
#     enable_length_check: bool = False
# ) -> None:
#     """
#     Runs Great Expectations checks on a Spark DataFrame.
#     """
#     import great_expectations as gx
#     from great_expectations.checkpoint import Checkpoint
#     # 1) Build a transient GX context and Spark datasource
#     context = gx.get_context()
#     ds = context.data_sources.add_spark(name="spark_in_memory")
#     asset = ds.add_dataframe_asset(name="df_asset")
#     batch_def = asset.add_batch_definition_whole_dataframe("df_batch")
#     batch = batch_def.get_batch(batch_parameters={"dataframe": df})
#     # context_root_dir = '/Volumes/ong_streamworkspace_37859/default/great_expectations/'
#     # context = gx.get_context(context_root_dir=context_root_dir)



#     # datasource = context.data_sources.add_or_update_spark(
#     #     name="my_spark_in_memory_datasource",
#     # )

#     # dataframe_asset = datasource.add_dataframe_asset(
#     #     name="ong-welldata"
#     # )


#     # batch_parameters = {"dataframe": landing_well_events}



#     # 2) Run expectations per schema
#     from great_expectations import expectations as E
#     results = []
#     ordered_cols = []
#     for col, props in schema.items():
#         ordered_cols.append(col)

#         if props.get("unique", False):
#             results.append(batch.validate(E.ExpectColumnValuesToBeUnique(column=col)))
#         if props.get("nullable", True) is False:
#             results.append(batch.validate(E.ExpectColumnValuesToNotBeNull(column=col)))

#         dtype = props.get("dtype")
#         if dtype:
#             results.append(batch.validate(E.ExpectColumnValuesToBeOfType(column=col, type_=dtype)))

#         if enable_length_check:
#             size = props.get("size")
#             if size is not None:
#                 results.append(
#                     batch.validate(
#                         E.ExpectColumnValueLengthsToBeBetween(
#                             column=col, min_value=None, max_value=int(size), strict_max=True
#                         )
#                     )
#                 )

#     # 3) Table-level expectations
#     if check_ordered_columns:
#         results.append(batch.validate(E.ExpectTableColumnsToMatchOrderedList(column_list=ordered_cols)))
#     if expected_row_count is not None:
#         results.append(batch.validate(E.ExpectTableRowCountToEqual(value=int(expected_row_count))))

#     # 4) Summarize results
#     total = len(results)
#     successes = sum(1 for r in results if getattr(r, "success", False))
#     failures = total - successes

#     print(f"[DQ] Expectations run: {total} | Passed: {successes} | Failed: {failures}")
#     if failures > 0:
#         for r in results:
#             if not getattr(r, "success", False):
#                 cfg = getattr(r, "expectation_config", None)
#                 etype = getattr(cfg, "type", "unknown") if cfg else "unknown"
#                 kwargs = getattr(cfg, "kwargs", {}) if cfg else {}
#                 print(f"[DQ][FAIL] {etype} {kwargs}")
#         raise Exception("Data Quality validation failed.")
#     else:
#         print("[DQ] All checks passed ✔️")

  

# COMMAND ----------

# validate_with_gx(
#     df=landing_well_events,
#     schema=producer_well_telemetry_schema,
#     expected_row_count=None,
#     check_ordered_columns=True,
#     enable_length_check=False
# )

# COMMAND ----------



    
# # Create a list of one or more Validation Definitions for the Checkpoint to run
# validation_definitions = [
#     context.validation_definitions.get("my_validation_definition")
# ]



# # Create an Expectation Suite
# suite_name = "my_expectation_suite"
# suite = gx.ExpectationSuite(name=suite_name)

# # Add the Expectation Suite to the Data Context
# suite = context.suites.add(suite)      


# from great_expectations.checkpoint import (
#     SlackNotificationAction,
#     UpdateDataDocsAction,
# )


# # Create a list of Actions for the Checkpoint to perform
# action_list = [
#     # This Action sends a Slack Notification if an Expectation fails.
#     SlackNotificationAction(
#         name="send_slack_notification_on_failed_expectations",
#         slack_token="${validation_notification_slack_webhook}",
#         slack_channel="${validation_notification_slack_channel}",
#         notify_on="failure",
#         show_failed_expectations=True,
#     ),
#     # This Action updates the Data Docs static website with the Validation
#     #   Results after the Checkpoint is run.
#     UpdateDataDocsAction(
#         name="update_all_data_docs",
#     ),
# ]

# # Create the Checkpoint
# checkpoint_name = "my_checkpoint"
# checkpoint = gx.Checkpoint(
#     name=checkpoint_name,
#     validation_definitions=validation_definitions,
#     actions=action_list,
#     result_format={"result_format": "COMPLETE"},
# )

# # Save the Checkpoint to the Data Context
# context.checkpoints.add(checkpoint)

# # Retrieve the Checkpoint later
# checkpoint_name = "my_checkpoint"
# checkpoint = context.checkpoints.get(checkpoint_name)



# COMMAND ----------


# spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet") \
#     .option("cloudFiles.schemaLocation", src_checkpointpath) \
#     .load(files_path).writeStream \
# .outputMode("append") \
# .partitionBy("client_id")
# .option("checkpointLocation", dest_checkpointpath) \
# .trigger(availableNow=True) \
# .toTable(dest_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE outlake.raw.well_events
# MAGIC SCHEDULE EVERY 1 HOUR
# MAGIC AS SELECT * FROM STREAM read_delta(src_tables[5]);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE outlake.raw.dailyproduction_events
# MAGIC SCHEDULE EVERY 1 HOUR
# MAGIC AS SELECT * FROM STREAM read_delta(src_tables[0]);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE outlake.raw.reservoir_events
# MAGIC SCHEDULE EVERY 1 HOUR
# MAGIC AS SELECT * FROM STREAM read_delta(src_tables[1]);

# COMMAND ----------

spark.sql(f"""
    CREATE OR REFRESH STREAMING TABLE dest_table
    AS SELECT * FROM STREAM read_delta(source_table);""")