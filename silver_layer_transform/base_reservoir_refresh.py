from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.listener import QueryStartedEvent, QueryProgressEvent, QueryTerminatedEvent
import argparse
import logging
import json


# Enable adaptive query execution for better join performance
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# Optimize shuffle for stream-stream joins
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

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
x = f"ingestion_to_landing_zone_script-v{vrsn}"
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

# Retrieve workspace name from Databricks secrets and format it
secret_name = str(dbutils.secrets.get(scope='databricks-keyvault', key='databricks-workspace-name')).lower()
workspace_name = secret_name.replace("-", "_")
SCHEMA1  = f"{workspace_name}.raw"
SCHEMA2  = f"{workspace_name}.base"

# Define volume path for checkpoints
VOLUME_PATH = f"/Volumes/{workspace_name}/default"

# Set source and destination schemas
sourcezone = SCHEMA1
destzone = SCHEMA2

# Define source and destination table names
source_table1 = f'{sourcezone}.`reservoir`'
source_table2 = f'{sourcezone}.`wellbore`'
checkPoint = f'{VOLUME_PATH}/checkpoints/base/reservoir_analytics'
dest_table = f'{destzone}.reservoir_analytics'

# Debug: Print table and checkpoint paths
logger.info(f"Source Table 1: {source_table1}")
# print(f"[DEBUG] Source Table 2: {source_table2}")
# print(f"[DEBUG] Destination Table: {dest_table}")
# print(f"[DEBUG] Checkpoint Path: {checkPoint}")

# Read and transform reservoir stream
# Removed dropDuplicates() - expensive stateful operation in streaming
reservoir_df = ()
