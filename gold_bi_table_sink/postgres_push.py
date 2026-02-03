from pyspark.sql.types import *
from pyspark.sql.functions import *

secret_name = str(dbutils.secrets.get(scope='databricks-keyvault', key='databricks-workspace-name')).lower()
workspace_name = secret_name.replace("-", "_")
SCHEMA1  = f"{workspace_name}.raw"
SCHEMA2  = f"{workspace_name}.base"


df = spark.read.table(f"{SCHEMA2}.well_monitoring")

database_host = "ep-shy-grass-a9d9sdtz-pooler.gwc.azure.neon.tech"
database_port = "5432" # update if you use a non-default port
database_name = "georesearchpartner"
table = "well-monitoring"
user = "neondb_user"
password = "npg_p7Vg9cmUtqLn"

(df.write
     .format("jdbc")
     .option("url", "jdbc:postgresql://{database_host}:{database_port}/{database_name}?sslmode=require&channel_binding=require")
     .option("dbtable", table)
     .option("user", user)
     .option("password", password)
     .option("driver", "org.postgresql.Driver")
     .mode("append")
     .option("truncate", "false")
     .save())