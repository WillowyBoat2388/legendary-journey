from pyspark.sql.types import *
from pyspark.sql.functions import *

# Retrieve the workspace name from Databricks secrets and format it for schema usage
secret_name = str(dbutils.secrets.get(scope='databricks-keyvault', key='databricks-workspace-name')).lower()
workspace_name = secret_name.replace("-", "_")
SCHEMA1 = f"{workspace_name}.base"
SCHEMA2 = f"{workspace_name}.serving"

# Retrieve the Postgres connection string from Databricks secrets
database = dbutils.secrets.get(scope='databricks-keyvault', key='postgres-sink')


# Get the max timestamp from Postgres to filter only new records
max_timestamp_query = "(SELECT COALESCE(MAX(timestamp), '1970-01-01'::timestamp) as max_ts FROM well_monitoring) as subq"
max_timestamp_df = spark.read \
    .format("jdbc") \
    .option("url", database) \
    .option("dbtable", max_timestamp_query) \
    .option("driver", "org.postgresql.Driver") \
    .load()
max_timestamp = max_timestamp_df.collect()[0][0]
print(f"Begining Reading and Filtering. Current Most Recent Timestamp Inserted: {max_timestamp}")
# Read data from the well_monitoring and firm_info tables in Spark
df1 = spark.read.table(f"{SCHEMA2}.well_monitoring").filter(col("timestamp") > lit(max_timestamp))

df2 = spark.read.table(f"{SCHEMA1}.firm_info").orderBy("FIRM_ID","FACILITY_ID")

# Read existing firm_info records from Postgres to identify what's already there
existing_firm_info = spark.read \
    .format("jdbc") \
    .option("url", database) \
    .option("dbtable", "firm_info") \
    .option("driver", "org.postgresql.Driver") \
    .load() \
    .select("firm_id", "facility_id")

# Filter df2 to only include records that don't already exist in Postgres
df2_new = df2.join(existing_firm_info, ["firm_id", "facility_id"], "left_anti")

database_host = database
table1 = "well_monitoring"
table2 = "firm_info"
print("Beginning Write to Postgres DF1(Well_monitoring)")
# Write df1 to the well_monitoring table in Postgres using JDBC
(df1.write
     .format("jdbc")
     .option("url", f"{database_host}")
     .option("dbtable", table1)
     .option("driver", "org.postgresql.Driver")
     .mode("append")
     .option("truncate", "false")
     .save())
print(f"Beginning Write to Postgres DF2(Firm Info) - {df2_new.count()} new records")
# Write df2_new to the firm_info table in Postgres using JDBC (only new records)
(df2_new.write
     .format("jdbc")
     .option("url", f"{database}")
     .option("dbtable", table2)
     .option("driver", "org.postgresql.Driver")
     .mode("append")
     .option("truncate", "false")
     .save())
