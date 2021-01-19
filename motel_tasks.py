import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, to_timestamp, date_format

INPUT_DIR = './input'
BIDS_PATH = os.path.join(INPUT_DIR, 'bids.txt')
EXCHANGE_RATE_PATH = os.path.join(INPUT_DIR, 'exchange_rate.txt')
MOTELS_PATH = os.path.join(INPUT_DIR, 'motels.txt')


def filter_erroneous_records(bids_df):
    bids_df = bids_df.select(split(bids_df['value'], ',', 18).alias('arr'))

    column_names = ["MotelID", "BidDate", "HU", "UK", "NL", "US", "MX", "AU",
                    "CA", "CN", "KR", "BE", "I", "JP", "IN", "HN", "GY", "DE"]

    select_statement = [col('arr').getItem(i).alias(column_names[i]) for i in range(18)]

    bids_df = bids_df.select(
        *select_statement
    )

    erroneous_bids = bids_df.select('*').where(col('HU').startswith('ERROR_'))

    grouped_erroneous_bids = erroneous_bids.groupby(['BidDate', 'HU'])
    erroneous_bids_count = grouped_erroneous_bids.count()
    erroneous_bids_count = erroneous_bids_count \
        .withColumnRenamed('HU', 'ErrorType') \
        .withColumn('count', col('count').astype('string'))

    erroneous_bids_count.write.mode('overwrite').csv('bids_error_count_csv')

    return bids_df.select('*').where(~col('HU').startswith('ERROR_'))


def main():
    spark = SparkSession \
        .builder \
        .appName('Motels Task') \
        .getOrCreate()

    bids_df = spark.read.text(BIDS_PATH)
    cleared_bids_df = filter_erroneous_records(bids_df)


if __name__ == '__main__':
    main()
