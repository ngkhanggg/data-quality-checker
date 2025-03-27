import json

from abc import ABC, abstractmethod

from pyspark.sql.functions import col, concat_ws, sha2

# My modules
from config.dq_config import DQConfig


def concat_hash_columns(df, df_columns, column_name):
    new_df = df.withColumn(column_name, sha2(concat_ws('', *[col(c).cast('string') for c in df_columns]), 256))
    return new_df


class DQTool(ABC):
    def __init__(self, logger, spark, dq_config: DQConfig):
        self.logger = logger
        self.spark = spark
        self.dq_config = dq_config
        self.source_data = None
        self.dest_data = None
        self.hashed_source_data = None
        self.hashed_dest_data = None

    def get_data_source(self, source_type: str):
        connection = ''
        database_name = ''
        table_name = ''

        source_mapping = {
            'source': (self.dq_config.source_connection, self.dq_config.source_database, self.dq_config.source_table),
            'dest': (self.dq_config.dest_connection, self.dq_config.dest_database, self.dq_config.dest_table),
        }

        if source_type in source_mapping:
            connection, database_name, table_name = source_mapping[source_type]
        else:
            self.logger.exception('dq_check_logger - Invalid source type')

        if connection == '' or connection is None:
            self.logger.info('dq_check_logger - A connection was not provided, reading from glue_catalog')
            df = self.spark.read.table(f"{database_name}.{table_name}")
        else:
            self.logger.info('dq_check_logger - A connection was provided, reading from glue_connection')
            df = None

        return df

    @abstractmethod
    def run(self):
        self.source_data = self.get_data_source('source')
        self.dest_data = self.get_data_source('dest')
