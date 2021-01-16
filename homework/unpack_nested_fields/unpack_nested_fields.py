from pyspark.sql.dataframe import DataFrame, ArrayType, StructType
from pyspark.sql.functions import col, array, explode


class UnpackNestedFields:
    """
    Class provides possibilities to unpack nested structures in row recursively and provide flat structure as result.
    To clarify rules, please investigate tests.
    After unpacking of structure additional columns should be provided with next name {struct_name}.{struct_field_name}

    """

    def unpack_nested(self, dataframe: DataFrame):
        columns_to_select = []
        for field in dataframe.schema.fields:
            if type(field.dataType) in (ArrayType, StructType):
                c = explode(field.name).alias('int_array')
            else:
                c = col(field.name)
            columns_to_select.append(c)
        return dataframe.select(*columns_to_select)
