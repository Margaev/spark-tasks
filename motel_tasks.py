import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import split, col, round

INPUT_DIR = './input'
BIDS_PATH = os.path.join(INPUT_DIR, 'bids.txt')
EXCHANGE_RATE_PATH = os.path.join(INPUT_DIR, 'exchange_rate.txt')
MOTELS_PATH = os.path.join(INPUT_DIR, 'motels.txt')


def string_to_columns(df, column_names):
    df = df.select(split(df['value'], ',', len(column_names)).alias('arr'))
    select_statement = [col('arr').getItem(i).alias(column_names[i]) for i in range(len(column_names))]

    return df.select(
        *select_statement
    )


def filter_erroneous_records(bids_df):
    erroneous_bids = bids_df.select('*').where(col('HU').startswith('ERROR_'))

    grouped_erroneous_bids = erroneous_bids.groupby(['BidDate', 'HU'])
    erroneous_bids_count = grouped_erroneous_bids.count()
    erroneous_bids_count = erroneous_bids_count \
        .withColumnRenamed('HU', 'ErrorType') \
        .withColumn('count', col('count').astype('string'))

    erroneous_bids_count.write.mode('overwrite').csv('bids_error_count_csv')

    return bids_df.select('*').where(~col('HU').startswith('ERROR_'))


def get_mapper_df(exchange_rate_df):
    return exchange_rate_df.select('ValidFrom', 'ExchangeRate')


def convert_currency_usd_to_eur(df: DataFrame, mapper_df: DataFrame, currency_columns):
    df = df.join(mapper_df, df['BidDate'] == mapper_df['ValidFrom'], 'left')

    for c in currency_columns:
        df = df.withColumn(c, round(col(c) * col('ExchangeRate'), 3))

    return df.select('MotelID', 'BidDate', *currency_columns)


def main():
    spark = SparkSession \
        .builder \
        .appName('Motels Task') \
        .getOrCreate()

    bids_df = spark.read.text(BIDS_PATH)

    bids_column_names = ['MotelID', 'BidDate', 'HU', 'UK', 'NL', 'US', 'MX', 'AU',
                         'CA', 'CN', 'KR', 'BE', 'I', 'JP', 'IN', 'HN', 'GY', 'DE']

    bids_df = string_to_columns(bids_df, bids_column_names)

    cleared_bids_df = filter_erroneous_records(bids_df)

    exchange_rate_df = spark.read.text(EXCHANGE_RATE_PATH)
    exchange_rate_column_names = ['ValidFrom', 'CurrencyName', 'CurrencyCode', 'ExchangeRate']
    exchange_rate_df = string_to_columns(exchange_rate_df, exchange_rate_column_names)

    mapper = get_mapper_df(exchange_rate_df)
    cleared_bids_df_eur = convert_currency_usd_to_eur(
        cleared_bids_df,
        mapper,
        ['HU', 'UK', 'NL', 'US', 'MX', 'AU', 'CA', 'CN', 'KR', 'BE', 'I', 'JP', 'IN', 'HN', 'GY', 'DE']
    )


if __name__ == '__main__':
    main()
