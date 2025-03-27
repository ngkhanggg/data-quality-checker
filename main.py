# ========================= Libs =========================

import logging
import sys

from pyspark.context import SparkConf, SparkContext
from pyspark.sql import SparkSession

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ========================= Logger =========================

formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s: %(message)s')
logger = logging.getLogger('dq_check_logger')
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# ========================= GlueJobArgs =========================

glue_job_args = getResolvedOptions(sys.argv, [
    'JOB_NAME',

    # REQUIREMENTS FOR SPARK_CONF
    'p_account_id',
    'p_raw_bucket',

    # REQUIREMENTS FOR CONFIG
    'config_database',
    'config_table',

    # REQUIREMENTS FOR ETL
    'job_id',
    'job_group_id'
])

# ========================= Spark & GlueContext =========================

list_spark_conf = [
    ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
    ("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"),
    ("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"),
    ("spark.sql.catalog.glue_catalog.glue.id", account_id),
    ("spark.sql.catalog.glue_catalog.glue.lakeformation-enabled", "true"),
    ("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
    ("spark.sql.catalog.glue_catalog.warehouse", f"s3://{raw_bucket}/")
]

spark_conf = SparkConf().setAll(list_spark_conf)
spark_context = SparkContext(conf=spark_conf)
spark_session = SparkSession(spark_context).builder.enableHiveSupport().getOrCreate()

glue_context = GlueContext(spark_context)

job = Job(glue_context)
job.init(glue_job_args['JOB_NAME'], glue_job_args)

# ========================= ListConfig =========================

config_database = glue_job_args['config_database']
config_table = glue_job_args['config_table']

config_table_query = f"""
    select *
    from glue_catalog.{config_database}.{config_table}
    order by id asc
"""

df = spark_session.sql(config_table_query)

list_config = df.rdd.map(lambda r: r.asDict()).collect()

del df

# ========================= Main =========================

job.commit()
