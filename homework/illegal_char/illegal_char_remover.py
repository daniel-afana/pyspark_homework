from pyspark.sql.dataframe import DataFrame
from typing import List

from pyspark.sql.functions import col, translate


class IllegalCharRemover:
    """
    Class provides possibilities to remove illegal chars from string column.
    """
    def __init__(self, chars: List[str], replacement):
        if not chars or replacement is None:
            raise ValueError
        self.chars = chars
        self.replacament = replacement

    def remove_illegal_chars(self, dataframe: DataFrame, source_column: str, target_column: str):
        df2 = dataframe.select(
            col('id'),
            translate(
                col(source_column), f'["".join({self.chars})]', self.replacament
            ).alias(target_column)
        )
        return df2.select('id', 'string_filtered')
