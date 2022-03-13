from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import sqlalchemy
from datetime import datetime
import argparse

# Parsing the input arguments
parser = argparse.ArgumentParser()
parser.add_argument("--app-name", help="Spark bulk application name",)
parser.add_argument("--file-path", help="Input file path",)
args = parser.parse_args()
app_name = args.app_name
file_path = args.file_path

# Application Constants
prefix = 'transient/{}/'.format(str(uuid.uuid4()))
author = 'vishal'
null_type = '@NULL@'
output_table_name = 'sample'
output_file_format = "CSV GZIP"

# Config values fetched from secret manager
username = '< user name >'
password = '< Password >'
url = '< Cluster host url >'
port = '< Cluster port >'
database = '< Cluster database name >'
iam_role = '< Cluster associated IAM role >'

# These variables are modified for the HDFS flow.
cluster_id = '< EMR Cluster ID >'
temp_dir_path = "hdfs:///{}".format(prefix)
final_path = 'emr://{}/'.format(cluster_id) + prefix + 'part*'

# Building the spark session
execution_start_time = datetime.now()
spark = SparkSession.builder.appName(app_name).getOrCreate()
spark_context = spark.sparkContext

# Reading input file from S3 path
df_csv = spark.read.format("csv") \
    .option("header", True) \
    .option("multiLine", True) \
    .option("ignoreLeadingWhiteSpace", True) \
    .option("ignoreTrailingWhiteSpace", True) \
    .option("escape", "\\") \
    .option("quote", "\"") \
    .load(file_path)

# Performing minimal transformation
modified_df = df_csv.withColumn('load_by', F.lit(author))

# Writing modified dataframe to HDFS
modified_df.write \
    .format("csv") \
    .option("escape", "\"") \
    .option("nullValue", null_type) \
    .option("compression", "gzip") \
    .option("header", True) \
    .mode("append").save(temp_dir_path)

# Establishing Redshift connection using sqlalchemy
postgresql_url = "postgresql://{}:{}@{}:{}/{}".format(
    username, password, url, port, database)
connection_engine = create_engine(
    postgresql_url, pool_size=0, max_overflow=-1, pool_pre_ping=True).execution_options(autocommit=True)
connection = Session(bind=connection_engine,
                     expire_on_commit=False, autocommit=True).connection()

# Building the COPY command
columns = ','.join(modified_df.columns)
query = "COPY {} ({}) FROM '{}' iam_role '{}' FORMAT AS {} NULL AS '{}' TIMEFORMAT 'auto' IGNOREHEADER 1;"
final_query = query.format(output_table_name, columns,
                           final_path, iam_role, output_file_format, null_type)

# Executing the COPY command
connection.execute(sqlalchemy.text(final_query))

total_time = str((datetime.now() - execution_start_time).total_seconds())
print('Total time taken {} seconds'.format(total_time))
