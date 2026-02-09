from pyspark.sql.types import *
from pyspark.sql.functions import *

# Retrieve the workspace name from Databricks secrets and format it for schema usage
secret_name = str(dbutils.secrets.get(scope='databricks-keyvault', key='databricks-workspace-name')).lower()
workspace_name = secret_name.replace("-", "_")
SCHEMA1 = f"{workspace_name}.base"
SCHEMA2 = f"{workspace_name}.serving"

# Retrieve the Postgres connection string from Databricks secrets
database = dbutils.secrets.get(scope='databricks-keyvault', key='postgres-sink')

# Read data from the well_monitoring and firm_info tables in Spark
df1 = spark.read.table(f"{SCHEMA2}.well_monitoring")
df2 = spark.read.table(f"{SCHEMA1}.firm_info").orderBy("FIRM_ID","FACILITY_ID")

database_host = database
table1 = "well_monitoring"
table2 = "firm_info"

# Write df1 to the well_monitoring table in Postgres using JDBC
(df1.write
     .format("jdbc")
     .option("url", f"{database_host}")
     .option("dbtable", table1)
     .option("driver", "org.postgresql.Driver")
     .mode("append")
     .option("truncate", "false")
     .save())

# Write df2 to the firm_info table in Postgres using JDBC
(df2.write
     .format("jdbc")
     .option("url", f"{database}")
     .option("dbtable", table2)
     .option("driver", "org.postgresql.Driver")
     .mode("append")
     .option("truncate", "false")
     .save())