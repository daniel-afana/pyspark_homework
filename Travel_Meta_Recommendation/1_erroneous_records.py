from pprint import pprint

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


if __name__ == '__main__':
    # Spark session & context
    spark = SparkSession.builder.master('local').getOrCreate()

    df = spark.read.csv('input/bids.txt')
    df = df.filter(col('_c2').startswith('ERROR_'))
    df = df.select('_c0', '_c1', '_c2')
    df = df.groupby('_c1', '_c2').agg({'*': 'count'})
    df.toPandas().to_csv('output/1_erroneus_records.txt', header=False, index=False)

