import json

from abc import ABC, abstractmethod

# My modules
from config.dq_config import DQConfig


class DQTool(ABC):
    def __init__(self, logger, spark, dq_config: DQConfig):
        self.logger = logger
        self.spark = spark
        self.dq_config = dq_config

    @abstractmethod
    def run(self):
        ...

    def __str__(self):
        return json.dumps(self.__dict__, indent=4)
