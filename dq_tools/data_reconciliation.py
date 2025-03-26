from pyspark.sql.functions import col

from dq_tools.dq_tool import DQTool


class DataReconciliation(DQTool):
    # Missing records
    # The primary_key is in source but not in dest
    def get_missing_records(self):
        return self.hashed_source_data.join(
            other=self.hashed_dest_data,
            on='primary_key',
            how='left_anti'
        )

    # Invalid records
    # The same primary_key is in both source and dest, but their hash_key columns are not the same
    def get_invalid_records(self):
        merged_data = self.hashed_source_data.alias('src').join(
            other=self.hashed_dest_data.alias('dest'),
            on='primary_key',
            how='inner'
        )

        invalid_records = merged_data.filter(col('src.hash_key') != col('dest.hash_key'))

        return invalid_records

    def run(self):
        pass
