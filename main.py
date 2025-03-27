# ========================= Libs =========================

import logging
import sys

from abc import ABC, abstractmethod

from pyspark.context import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, sha2

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

# ========================= Class DQConfig =========================

class DQConfig:
    def __init__(self):
        try:
            self.id: int = int(dict_config['id'])
            self.type: int = int(dict_config['type'])
            self.group_id: int = int(dict_config['group_id'])
            self.source_system: str = dict_config['source_system']

            self.source_columns: list[str] = dict_config['source_columns'].split(',')
            self.source_incr_columns: list[str] = dict_config['source_incr_columns'].split(',')
            self.source_filters: list[str] = dict_config['source_filters'].split(',')
            self.source_biz_keys: list[str] = dict_config['source_biz_keys'].split(',')

            self.dest_columns: list[str] = dict_config['dest_columns'].split(',')
            self.dest_incr_columns: list[str] = dict_config['dest_incr_columns'].split(',')
            self.dest_filters: list[str] = dict_config['dest_filters'].split(',')
            self.dest_biz_keys: list[str] = dict_config['dest_biz_keys'].split(',')

            self.threshold: str = dict_config['threshold']

            self.source_connection: str = dict_config['source_connection']
            self.source_database: str = dict_config['source_database']
            self.source_table: str = dict_config['source_table']

            self.dest_connection: str = dict_config['dest_connection']
            self.dest_database: str = dict_config['dest_database']
            self.dest_table: str = dict_config['dest_table']
        except ValueError as ve:
            print(f"A ValueError was raised while getting config_table: {ve}")
            raise ve
        except Exception as e:
            print(f"An Exception was raised while getting config_table: {e}")
            raise e

    def is_config_valid(self):
        errors = [
            (len(self.source_columns) != len(self.dest_columns), 'Source and Destination columns do not match'),
            (len(self.source_biz_keys) != len(self.dest_biz_keys), 'Source and Destination biz keys do not match'),
            (self.source_database == '' or self.source_database is None, 'Source database cannot be empty'),
            (self.source_table == '' or self.source_table is None, 'Source table cannot be empty'),
            (self.dest_database == '' or self.dest_database is None, 'Destination database cannot be empty'),
            (self.dest_table == '' or self.dest_table is None, 'Destination table cannot be empty')
        ]

        for condition, message in errors:
            if condition:
                print(f"Invalid config: {message}")

        return not any(condition for condition, message in errors)

# ========================= Class DQTool =========================

class DQTool(ABC):
    def __init__(self, logger, spark_session, dq_config):
        self.logger = logger
        self.spark_session = spark_session
        self.dq_config = dq_config

        self.source_data = None
        self.dest_data = None
        self.hashed_source_data = None
        self.hash_dest_data = None

    def get_data_source(self, source_type):
        connection = ''
        database = ''
        table = ''

        source_mapping = {
            'source': (self.dq_config.source_connection, self.dq_config.source_database, self.dq_config.source_table),
            'dest': (self.dq_config.dest_connection, self.dq_config.dest_database, self.dq_config.dest_table)
        }

        if source_type in source_mapping:
            connection, database, table = source_mapping[source_type]
        else:
            self.logger('dq_check_logger - Invalid source_type')

        if connection == '' or connection is None:
            self.logger.info('dq_check_logger - A connection was not provided, start reading from glue_catalog')
            pass
        else:
            self.logger.info('dq_check_logger - A connection was provided, start reading from glue_connection')
            pass

    def concat_hash_columns(self, df, df_columns, column_name):
        return df.withColumn(column_name, sha2(concat_ws('', *[col(c).cast('string') for c in df_columns]), 256))

    @abstractmethod
    def run(self):
        self.source_data = get_data_source('source')
        self.dest_data = get_data_source('dest')

# ========================= Main =========================

job.commit()
