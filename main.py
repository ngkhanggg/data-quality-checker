import concurrent.futures
import logging
import sys

from pyspark.context import SparkConf, SparkContext
from pyspark.sql import SparkSession

from awsglue.utils import getResolvedOptions

# My modules
from config.dq_config import DQConfig

# ================================================== SparkConfig ==================================================

class MySpark:
    def __init__(self, account_id, bucket):
        self.list_conf = [
            ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
            ("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"),
            ("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"),
            ("spark.sql.catalog.glue_catalog.glue.id", account_id),
            ("spark.sql.catalog.glue_catalog.glue.lakeformation-enabled", "true"),
            ("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
            ("spark.sql.catalog.glue_catalog.warehouse", f"s3://{self.bucket_name}/")
        ]
        self.context = None
        self.session = None

    def config_session(self):
        spark_conf = SparkConf().setAll(self.list_conf)
        context = SparkContext(conf=spark_conf)
        session = SparkSession(context).builder.enableHiveSupport().getOrCreate()
        self.context = context
        self.session = session

    def get_session(self):
        return self.context, self.session

# ================================================== Logger Setup ==================================================

formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(filename)s:%(lineno)s: %(message)s')
logger = logging.getLogger('dq_check_logger')
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# ================================================== GlueJobArgs Setup ==================================================

glue_job_args = getResolvedOptions(sys.argv, [
    'JOB_NAME',

    # REQUIREMENTS TO CONFIG SPARK
    'p_account_id',
    'p_raw_bucket',

    # REQUIREMENTS TO CONFIG JOB
    'dq_check_id',
    'dq_check_group_id'
    'dq_config_database',
    'dq_config_table'
])

# ================================================== Spark Setup ==================================================

account_id = glue_job_args['p_account_id']
raw_bucket = glue_job_args['p_raw_bucket']

my_spark = MySpark(account_id, raw_bucket)
my_spark.config_session()
sc, spark = my_spark.get_session()
spark.sql('use glue_catalog')

# ====================================================================================================

def get_list_config():
    global glue_job_args

    config_id = glue_job_args['dq_check_id']
    config_group_id = glue_job_args['dq_check_group_id']
    database = glue_job_args['dq_config_database']
    table = glue_job_args['dq_config_table']

    where_clause = f"group_id = {config_group_id}" if config_id == -1 else f"group_id = {config_group_id} and id = {config_id}"

    query = f"""
        select *
        from {database}.{table}
        where {where_clause}
        order by id asc
    """

    df = spark.sql(query)
    list_config = df.rdd.map(lambda r: r.asDict()).collect()
    return list_config


def start_dq(list_config):
    global logger, glue_job_args, spark

    for item in list_config:
        dq_config = DQConfig(logger, item)

        print(dq_config)


def main():
    list_config = get_list_config()

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(start_dq, list_config)]
        concurrent.futures.wait(futures)
        [future.result() for future in futures]

if __name__ == '__main__':
    main()
