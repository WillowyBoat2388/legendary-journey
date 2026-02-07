from pyspark.sql.types import *
from pyspark.sql.functions import *

secret_name = str(dbutils.secrets.get(scope='databricks-keyvault', key='databricks-workspace-name')).lower()
workspace_name = secret_name.replace("-", "_")
SCHEMA1 = f"{workspace_name}.base"
SCHEMA2 = f"{workspace_name}.serving"


df1 = spark.read.table(f"{SCHEMA2}.well_monitoring")
df2 = spark.read.table(f"{SCHEMA1}.well_monitoring")
database_host = "ep-shy-grass-a9d9sdtz-pooler.gwc.azure.neon.tech"
database_port = "5432" # update if you use a non-default port
database_name = "georesearchpartner"
table1 = "well_monitoring"
table2 = "firm_info"
user = "neondb_user"
password = "npg_p7Vg9cmUtqLn"

(df1.write
     .format("jdbc")
     .option("url", f"jdbc:postgresql://{database_host}:{database_port}/{database_name}?sslmode=require&channel_binding=require")
     .option("dbtable", table1)
     .option("user", user)
     .option("password", password)
     .option("driver", "org.postgresql.Driver")
     .mode("append")
     .option("truncate", "false")
     .save())

(df2.write
     .format("jdbc")
     .option("url", f"jdbc:postgresql://{database_host}:{database_port}/{database_name}?sslmode=require&channel_binding=require")
     .option("dbtable", table2)
     .option("user", user)
     .option("password", password)
     .option("driver", "org.postgresql.Driver")
     .mode("append")
     .option("truncate", "false")
     .save())