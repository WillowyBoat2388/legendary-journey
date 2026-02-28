from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.listener import QueryStartedEvent, QueryProgressEvent, QueryTerminatedEvent
import argparse
import logging
import json

# src_list = ['production-daily-data', 'reservoir', 'equipment-events', 'wellbore', 'facility-telemetry', 'well-telemetry']
# Set up structured JSON format for our application logs.

class JSONFormatter(logging.Formatter):
   """Structured JSON formatter for logging."""

   def format(self, record: logging.LogRecord) -> str:
       """Formats log records as JSON."""
       log_record = {
           'timestamp': self.formatTime(record, self.datefmt),
           'level': record.levelname,
           'message': record.getMessage(),
           'logger': record.name,
           'line': record.lineno,
       }
       return json.dumps(log_record)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a stream (console) handler
# and set the formatter for the handler
handler = logging.StreamHandler()
formatter = JSONFormatter(datefmt='%Y-%m-%dT%H:%M:%S') # ISO-8601 format
handler.setFormatter(formatter)
logger.addHandler(handler)
vrsn = 5
# Use the logger object to log messages instead of print()
x = f"ingestion_to_raw_zone_script-v{vrsn}"
logger.debug("This is a debug message")
logger.info(f"This is an info message. x = {x}")
logger.warning("This is a warning message")

class SparkStreamingLogger(StreamingQueryListener):
   def onQueryStarted(self, event: QueryStartedEvent):
       logger.info(f"Query started: {event.name} ({event.id})")

   def onQueryProgress(self, event: QueryProgressEvent):
       logger.info(f"Query progress: {event.progress.json}")

   def onQueryTerminated(self, event: QueryTerminatedEvent):
       if event.exception:
           logger.error(f"Query terminated: {event.id} ({event.exception})")
       else:
           logger.info(f"Query terminated: {event.id}")

# Register the logging listener
spark.streams.addListener(SparkStreamingLogger())


def requirements(table):
    # Retrieve workspace name from Databricks secret and format it
    secret_name = str(dbutils.secrets.get(scope='databricks-keyvault', key='databricks-workspace-name')).lower()
    workspace_name = secret_name.replace("-", "_")
    SCHEMA1  = f"{workspace_name}.landing"
    SCHEMA2  = f"{workspace_name}.raw"

    VOLUME_PATH = f"/Volumes/{workspace_name}/default"
    
    sourcezone = SCHEMA1
    destzone = SCHEMA2

    # Define source and destination table names
    source_table = f'{sourcezone}.`{table}`'
    dest_table = f'{destzone}.`{table}`'
    # src_list = ['production-daily-data', 'reservoir', 'equipment-events', 'wellbore', 'facility-telemetry', 'well-telemetry']
    
    # Define checkpoint path and schema contract path
    checkPoint = f"{VOLUME_PATH}/checkpoints/raw/{dest_table}"
    schema_json  = f"../resources/contracts/producer/{table}.json"

    # Load schema from JSON contract
    with open(schema_json, "r") as f:
        loaded_json_data = json.load(f)

    OnGProdContract = StructType.fromJson(loaded_json_data)
    
    return [OnGProdContract, source_table, dest_table, checkPoint]

def run(OnGProdContract, source_table, dest_table, checkPoint):
    # Ingest data from source_table to dest_table using clusterBy on client_id
    print("RAW ZONE SINKING STARTED")

    spark.readStream.schema(OnGProdContract).option("maxFilesPerTrigger", 1).table(source_table).writeStream.outputMode("append").option("checkpointLocation", checkPoint).clusterBy("client_id").trigger(availableNow=True).toTable(dest_table)

    print("RAW ZONE SINKING COMPLETED")

    return

def run2(OnGProdContract, source_table, dest_table, checkPoint):
    # Ingest data from source_table to dest_table using partitionBy on client_id
    print("RAW ZONE SINKING STARTED")

    spark.readStream.schema(OnGProdContract).option("maxFilesPerTrigger", 1).table(source_table).writeStream.outputMode("append").option("checkpointLocation", checkPoint).partitionBy("client_id").trigger(availableNow=True).toTable(dest_table)

    print("RAW ZONE SINKING COMPLETED")

    return

if __name__ == "__main__":
    # Parse command line argument for table name
    parser = argparse.ArgumentParser(
                    description='Ingest Stream Data')
    parser.add_argument('input',
                    type=str,
                    help = "The type of event data coming in")
    
    args = parser.parse_args()
    table = args.input
    
    req = requirements(table)
    # Use run2 for 'well-telemetry', otherwise use run
    if table == 'well-telemetry':
        run2(req[0], req[1], req[2], req[3])
    else:
        run(req[0], req[1], req[2], req[3])
    