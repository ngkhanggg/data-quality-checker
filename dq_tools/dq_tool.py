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

    @abstractmethod
    def run(self):
        ...

    def __str__(self):
        return json.dumps(self.__dict__, indent=4)
