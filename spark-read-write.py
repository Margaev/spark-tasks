import os

from pyspark.sql import SparkSession

# https://www.kaggle.com/stoney71/new-york-city-transport-statistics?select=mta_1706.csv

# spark-submit --packages org.apache.spark:spark-avro_2.12:3.0.1

INPUT_DATA_PATH = '###'
INPUT_FILE_NAME = 'mta_1706.csv'

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('PySpark read and write') \
        .getOrCreate()

    df = spark.read.csv(os.path.join(INPUT_DATA_PATH, INPUT_FILE_NAME), header=True)

    for c in df.columns:
        df = df.withColumnRenamed(c, c.replace('.', '_'))

    df.printSchema()
    df.show()

    # Write to Sequence File
    output_sequence_file_name = INPUT_FILE_NAME[:-4] + '_sequence'
    rdd_data = df.rdd.map(tuple)
    res = rdd_data.map(lambda line: (None, line))
    res.saveAsSequenceFile(output_sequence_file_name)

    # Write to Avro
    output_avro_file_name = INPUT_FILE_NAME[:-4] + '_avro'
    df.write.format('avro').save(output_avro_file_name)

    # # Write to Parquet
    output_parquet_file_name = INPUT_FILE_NAME[:-4] + '_parquet'
    df.write.parquet(output_parquet_file_name + '_gzip', compression='gzip')

    # output_parquet_file_name = INPUT_FILE_NAME[:-4] + '_parquet'
    # df.write.parquet(output_parquet_file_name + '_lz4', compression='lz4')

    output_parquet_file_name = INPUT_FILE_NAME[:-4] + '_parquet'
    df.write.parquet(output_parquet_file_name + '_snappy', compression='snappy')

    # Write to ORC
    output_orc_file_name = INPUT_FILE_NAME[:-4] + '_orc'
    df.write.orc(output_orc_file_name)
