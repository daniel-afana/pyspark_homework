from collections import Counter

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col


class SchemaMerging:
    """
    Class provides possibilities to union tow datasets with different schemas.
    Result dataset should contain all rows from both with columns from both dataset.
    If columns have the same name and type - they are identical.
    If columns have different types and the same name, 2 new column should be provided with next pattern:
    {field_name}_{field_type}
    """

    def union(self, dataframe1: DataFrame, dataframe2: DataFrame):
        df1_dtypes = set(dataframe1.dtypes)
        df2_dtypes = set(dataframe2.dtypes)
        intersection = df1_dtypes & df2_dtypes
        identical_columns_names = [i[0] for i in intersection]
        identical_columns_names = sorted(
            identical_columns_names,
            key=lambda x: dataframe1.columns.index(x)
        )

        symmetric_difference = df1_dtypes ^ df2_dtypes
        df1_unique = df1_dtypes & symmetric_difference
        df2_unique = df2_dtypes & symmetric_difference

        c = Counter(i[0] for i in symmetric_difference)
        common_names_with_different_types = [i for i, count in c.items() if count > 1]

        df1_special_columns_to_add = df1_unique.intersection(
            set(
                (i for i in symmetric_difference if i[0] in common_names_with_different_types)
            )
        )
        df1_special_columns_names = [f'{i[0]}_{i[1]}' for i in df1_special_columns_to_add]
        df1_unique -= df1_special_columns_to_add
        df1_unique_names = [i[0] for i in df1_unique]

        df2_special_columns_to_add = df2_unique.intersection(
            set(
                (i for i in symmetric_difference if i[0] in common_names_with_different_types)
            )
        )
        df2_special_columns_names = [f'{i[0]}_{i[1]}' for i in df2_special_columns_to_add]
        df2_unique -= df2_special_columns_to_add
        df2_unique_names = [i[0] for i in df2_unique]

        for col_name_to_add in df2_unique_names:
            dataframe1 = dataframe1.withColumn(col_name_to_add, lit(None))
        for col_name_to_add in df2_special_columns_names:
            dataframe1 = dataframe1.withColumn(col_name_to_add, lit(None))

        for col_name_to_add in df1_unique_names:
            dataframe2 = dataframe2.withColumn(col_name_to_add, lit(None))
        for col_name_to_add in df1_special_columns_names:
            dataframe2 = dataframe2.withColumn(col_name_to_add, lit(None))

        df1_special_columns_selection = [
            col(i[0][0]).alias(i[1]) for i in
            zip(df1_special_columns_to_add, df1_special_columns_names)
        ]
        df2_special_columns_selection = [
            col(i[0][0]).alias(i[1]) for i in
            zip(df2_special_columns_to_add, df2_special_columns_names)
        ]
        df1_cols_to_select = (
                identical_columns_names +
                df2_unique_names +
                df2_special_columns_names +
                (df1_special_columns_selection or df1_unique_names)
        )
        df2_cols_to_select = (
                identical_columns_names +
                df1_unique_names +
                df1_special_columns_names +
                (df2_special_columns_selection or df2_unique_names)
        )
        dataframe1_selection = dataframe1.select(*df1_cols_to_select)
        dataframe2_selection = dataframe2.select(*df2_cols_to_select)
        result = dataframe1_selection.unionByName(dataframe2_selection).distinct()

        return result
