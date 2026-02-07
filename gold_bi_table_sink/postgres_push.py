from pyspark.sql.types import *
from pyspark.sql.functions import *

secret_name = str(dbutils.secrets.get(scope='databricks-keyvault', key='databricks-workspace-name')).lower()
workspace_name = secret_name.replace("-", "_")
SCHEMA1 = f"{workspace_name}.base"
SCHEMA2 = f"{workspace_name}.serving"

database = dbutils.secrets.get(scope='databricks-keyvault', key='postgres-sink')
df1 = spark.read.table(f"{SCHEMA2}.well_monitoring")
df2 = spark.read.table(f"{SCHEMA1}.firm_info").orderBy("FIRM_ID","FACILITY_ID")

database_host = database
table1 = "well_monitoring"
table2 = "firm_info"

(df1.write
     .format("jdbc")
     .option("url", f"{database_host}")
     .option("dbtable", table1)
     .option("driver", "org.postgresql.Driver")
     .mode("append")
     .option("truncate", "false")
     .save())

(df2.write
     .format("jdbc")
     .option("url", f"{database}")
     .option("dbtable", table2)
     .option("driver", "org.postgresql.Driver")
     .mode("append")
     .option("truncate", "false")
     .save())
