from pyspark.sql import DataFrame
from pyspark.sql.functions import when, expr
from traceback_with_variables import prints_exc


class HistoryProduct:
    """
    Class provides possibilities to compute history of rows.
    You should compare old and new dataset and define next for each row:
     - row was changed
     - row was inserted
     - row was deleted
     - row not changed
     Result should contain all rows from both datasets and new column `meta`
     where status of each row should be provided ('not_changed', 'changed', 'inserted', 'deleted')
    """
    def __init__(self, primary_keys=None):
        pass

    @prints_exc
    def get_history_product(self, old_dataframe: DataFrame, new_dataframe: DataFrame):
        # We expect the same set of columns for both DF
        col_names = old_dataframe.columns
        # Exclude the column on which dataframes are joined
        col_names.remove('id')

        # Join DFs
        df_old = old_dataframe.alias("df_old")
        df_new = new_dataframe.alias("df_new")
        joined_df = df_old.join(df_new, on='id', how='outer')

        # Distinguish columns from old and new DF
        old_columns = [f'df_old.{c_name}' for c_name in col_names]
        new_columns = [f'df_new.{c_name}' for c_name in col_names]

        # Prepare expressions for finding the values in the new column
        old_columns_null_expr = ' IS NULL AND '.join(old_columns) + ' IS NULL'
        new_columns_null_expr = ' IS NULL AND '.join(new_columns) + ' IS NULL'
        equals_expr = ''
        for old_c, new_c in zip(old_columns, new_columns):
            equals_expr += f'{old_c} = {new_c} AND '
        equals_expr = equals_expr.rstrip(' AND ')

        # Add 'meta' column
        meta_col = when(
            expr(equals_expr), 'not_changed'
        ).when(
            expr(old_columns_null_expr), 'inserted'
        ).when(
            expr(new_columns_null_expr), 'deleted'
        ).otherwise('changed')
        joined_df = joined_df.withColumn('meta', meta_col)

        # Create final set of columns
        final_df = joined_df
        for c_name in col_names:
            c_name_new_column = when(
                expr('meta = "deleted"'), expr(f'df_old.{c_name}')
            ).otherwise(expr(f'df_new.{c_name}'))
            final_df = final_df.withColumn(f'new_{c_name}', c_name_new_column)

        # Drop temp columns and return result
        # result = joined_df.drop(*old_columns)  DOESN'T WORK! PIECE OF SHIT
        result = final_df
        for c_name in col_names:
            # result = result.drop(new_dataframe[c_name]) PIECE OF SHIT! WORKS ONLY WITH ONE COLUMN

            # I have to use this hack because of the weird Spark behavior (see pieces of shits above)
            result = result.drop(c_name)
            result = result.withColumn(c_name, expr(f'new_{c_name}'))
            result = result.drop(f'new_{c_name}')

        return result.select('id', 'name', 'score', 'meta')
