import json


class DQConfig:
    def __init__(self, logger, dict_config):
        self.logger = logger

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
            logger.exception(f"dq_check_logger - A ValueError was raised while getting config_table: {ve}")
            raise ve

        except Exception as e:
            logger.exception(f"dq_check_logger - An Exception was raised while getting config_table: {e}")
            raise e

    def __str__(self):
        return json.dumps(self.__dict__, indent=4)
