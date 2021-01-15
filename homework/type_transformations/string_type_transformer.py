import time

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, to_timestamp, udf
from pyspark.sql.types import StructType, DateType, IntegerType, BooleanType, DoubleType, DecimalType, TimestampType


class StringTypeTransformer:
    """
    Class provides transformation for columns with string types to other, where it possible.
    """

    def transform_dataframe(self, dataframe: DataFrame, expected_schema: StructType):
        spark = SparkSession.builder.master('local').getOrCreate()

        columns_to_select = []
        for field in expected_schema.fields:
            if type(field.dataType) == DateType:
                casted = to_date(col(field.name), 'dd-MM-yyyy').alias(field.name)
            elif type(field.dataType) == TimestampType:
                casted = to_timestamp(col(field.name), 'dd-MM-yyyy HH:mm:ss').alias(field.name)
            else:
                casted = col(field.name).cast(field.dataType)
            columns_to_select.append(casted)
        casted_df = dataframe.select(*columns_to_select)
        try:
            result_df = spark.createDataFrame(casted_df.collect(), expected_schema)
        except ValueError:
            result_df = spark.createDataFrame(casted_df.dropna().collect(), expected_schema)
        return result_df
