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

def requirements():
    # Retrieve workspace name from Databricks secret and format it for schema/volume usage
    secret_name = str(dbutils.secrets.get(scope='databricks-keyvault', key='databricks-workspace-name')).lower()
    workspace_name = secret_name.replace("-", "_")
    SCHEMA1  = f"{workspace_name}.landing"

    # Define base volume path
    VOLUME_PATH = f"/Volumes/{workspace_name}/default"

    # Define source and checkpoint paths for production data
    production_src = f"{VOLUME_PATH}/ong_sensorstream/output"
    destinationlakeCheckpoints = f"{VOLUME_PATH}/checkpoints/landing"
    # outlake = "outlake.landing"

    return production_src, destinationlakeCheckpoints, SCHEMA1

def telemetry_dump(production_source, source, checkPoints, outlake):
    # Print which source is being processed
    logger.info(f"{source.upper()} LANDINGZONE SINK")
    files_path = f'{production_source}/{source}'
    src_inferredSchema = f'{files_path}/_checkpoint'
    dest_checkpointpath = f'{checkPoints}/{source}'
    outputPath = f"{outlake}.`{source}`"

    logger.info("Streaming Begins")

    # Read streaming data from cloudFiles (parquet), infer schema, and write to Delta table with checkpointing
    spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet") \
        .option("cloudFiles.schemaLocation", src_inferredSchema) \
        .load(files_path).writeStream \
        .outputMode("append") \
        .option("checkpointLocation", dest_checkpointpath) \
        .trigger(availableNow=True) \
        .toTable(outputPath)
    logger.info("LANDING ZONE SINKING COMPLETED")

    return outputPath

if __name__ == "__main__":

    # Get required paths and schema
    production_src, destinationlakeCheckpoints, outlake = requirements()

    # Parse command-line argument for source type
    parser = argparse.ArgumentParser(
                    description='Ingest Stream Data')
    parser.add_argument('input',
                    type=str,
                    help = "The type of event data coming in")

    args = parser.parse_args()
    source = args.input

    # Start telemetry data ingestion
    telemetry_dump(production_src, source, destinationlakeCheckpoints, outlake)