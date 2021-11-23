import os
import pandas as pd
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, split, monotonically_increasing_id
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import udf
from datetime import datetime, timedelta
from pyspark.sql.functions import sequence, to_date, explode, col

config = configparser.ConfigParser()
config.read('config.cfg')

AWS_ACCESS_KEY_ID=config['AWS']['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY=config['AWS']['AWS_SECRET_ACCESS_KEY']


os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

## create spark session
def create_spark_session():

    """
        Creating a Apache spark session on EMR to process the input data.

        Output: Spark session.
    """

    spark = SparkSession \
                        .builder \
                        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,saurfang:spark-sas7bdat:3.0.0-s_2.12") \
                        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
                        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
                        .config('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY_ID) \
                        .config('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_ACCESS_KEY) \
                        .enableHiveSupport() \
                        .getOrCreate()

    return spark

## Define two data quality data checks

def data_null_checks(table,column):

    """
        check if the primary key column of a table has null values.

        Parameters:
        -------------
        table: the spark DataFrame that needs to be checked.
        column: the primary key or any other column that is not supposed to have null values.
    """

    table.createOrReplaceTempView("temp")


    null_count = table.filter(col(column).isNull()).count()
    exp_result = 0

    if exp_result != null_count:
        print('There are null values in the table!')
    else:
        print('Data Quality checks passed')

def data_unique_checks(table,column):

    """
        check if the primary key column of a table has unique values.

        Parameters:
        -------------
        table: the spark DataFrame that needs to be checked.
        column: the primary key or any other column that is not supposed to have unique values.
    """

    unique_count = table.select(column).distinct().count()
    row_count = table.count()

    if unique_count != row_count:
        print(f'There are duplicated values in the table')
    else:
        print('Data Quality checks passed')

## process files and create table parchuet files and write back to s3.

def process_airport_data(spark, input_data, output_data):

    """
        Process airport file and write back to S3 in parquet format

         Parameters
        ----------
        spark: spark session
        input_data : input file path in s3
        output_data : output file path in s3

    """

    # read airport data file
    df = spark.read.format('csv').load(input_data,header=True)

    # fill null values with na
    df = df.na.fill({'municipality': 'NA'})

    # filter to show only US airports and create table view
    df.filter(col('iso_country')=='US').createOrReplaceTempView("staging_airport_table")

    df_table = spark.sql("""SELECT
                                    ident        AS airport_id,
                                    name         AS airport_name,
                                    type         AS airport_type,
                                    iso_region   AS region,
                                    municipality AS municipality,
                                    CAST(SPLIT(coordinates,',')[0] AS double)  AS coordinate_x,
                                    CAST(SPLIT(coordinates,',')[1] AS double)  AS coordinate_y
                            FROM staging_airport_table ORDER BY region
                        """).dropDuplicates()

    data_null_checks(df_table,'airport_id')
    data_unique_checks(df_table,'airport_id')

    # write airport table to parquet files partitioned by iso_region
    df_table.write.partitionBy("region").parquet(output_data+"airport_table.parquet")

def process_country_data(spark, input_data, output_data):

    """
        Process country file and write back to S3 in parquet format

         Parameters
        ----------
        spark: spark session
        input_data : input file path in s3
        output_data : output file path in s3

    """

    # read country data file
    df = spark.read.format('csv').load(input_data,header=True)

    df_table = df.createOrReplaceTempView("staging_country_table")

    df_table = spark.sql("""SELECT
                                    Name        AS country_name,
                                    Code         AS country_code
                             FROM staging_country_table ORDER BY country_name
                            """).dropDuplicates()

    data_null_checks(df_table,'country_name')
    data_unique_checks(df_table,'country_name')

    # write country table to parquet files
    df_table.write.parquet(output_data+"country_table.parquet")

def process_region_data(spark, input_data, output_data):

    """
        Process region file and write back to S3 in parquet format

         Parameters
        ----------
        spark: spark session
        input_data : input file path in s3
        output_data : output file path in s3

    """

    # read region data file
    df = spark.read.format('csv').load(input_data,header=True)

    df = df.filter(col('country_code')=='US').withColumn('state_code',split(col('code'),'-')[1])

    df_table = df.createOrReplaceTempView("staging_region_table")

    df_table = spark.sql("""SELECT
                                subdivision_name        AS state_name,
                                state_code              AS state_code,
                                country_code            AS country_code,
                                code                    AS country_state
                            FROM staging_region_table ORDER BY state_name
                        """).dropDuplicates()

    data_null_checks(df_table,'country_state')
    data_unique_checks(df_table,'country_state')

    # write region table to parquet files
    df_table.write.parquet(output_data+"region_table.parquet")

def process_demographics_data(spark, input_data, output_data):

    """
        Process demographics file and write back to S3 in parquet format

         Parameters
        ----------
        spark: spark session
        input_data : input file path in s3
        output_data : output file path in s3

    """

    # read demographics data file
    df = spark.read.format('csv').load(input_data,header=True,sep=';')

    df = df.na.fill({\
                      'City':'NA', \
                      'State':'NA', \
                      'Median Age':0.0, \
                      'Male Population':0.0, \
                      'Female Population':0.0, \
                      'Total Population':0.0, \
                      'Number of Veterans':0.0, \
                      'Foreign-born':0.0, \
                      'Average Household Size':0.0, \
                      'State Code':'NA', \
                      'Race':'NA', \
                      'Count':0.0}).withColumn('demo_id',monotonically_increasing_id())

    df.createOrReplaceTempView("staging_demo_table")
    df_table = spark.sql("""SELECT
                                demo_id         AS id
                                City            AS city_name,
                                `State Code`    AS state_code,
                                `Median Age`    AS median_age,
                                `Male Population` AS male_population,
                                `Female Population` AS female_population,
                                `Total Population`  AS total_population,
                                `Foreign-born`      AS foreign_born,
                                `Average Household Size` AS avg_household_size,
                                `Race`          AS race,
                                `Count`         AS race_population
                         FROM staging_demo_table
                        """).dropDuplicates()

    data_null_checks(df_table,'id')
    data_unique_checks(df_table,'id')


    # write demographics table to parquet files
    df_table.write.partitionBy("state_code").parquet(output_data+"demographics_table.parquet")

def process_immigration_data(spark, input_data, output_data):

    """
        Process immigration file and write back to S3 in parquet format

         Parameters
        ----------
        spark: spark session
        input_data : input file path in s3
        output_data : output file path in s3

    """

    # read immigration data file
    df = spark.read.format("com.github.saurfang.sas.spark").load(input_data)

    df = df.na.fill({'depdate':0})

    ## define a udf to convert datetime
    convertTimeUDF = udf(lambda z: datetime(1960,1,1)+timedelta(days=z),DateType())

    ## convert datatime columns
    df = df.withColumn('arrdate',convertTimeUDF(col('arrdate'))) \
           .withColumn('depdate',convertTimeUDF(col('depdate')))

    df.createOrReplaceTempView("staging_immi_table")
    df_table = spark.sql("""SELECT
                                    cicid            AS immi_id,
                                    i94res           AS residency,
                                    i94port          AS entry_port,
                                    arrdate          AS arrival_date,
                                    i94mode          AS transportation,
                                    i94addr          AS arrival_state,
                                    depdate          AS departure_date,
                                    i94bir           AS age,
                                    i94visa          AS travel_purpose,
                                    biryear          AS birth_year,
                                    gender           AS gender,
                                    airline          AS airline,
                                    fltno            AS flight_number,
                                    visatype         AS visa_type
                            FROM staging_immi_table
                            """).dropDuplicates()

    data_null_checks(df_table,'immi_id')
    data_unique_checks(df_table,'immi_id')

    # write immigration table to parquet files
    df_table.write.partitionBy("residency",'arrival_state').parquet(output_data+"immigration_table.parquet")

def process_time_data(spark, output_data):

    """
         write time to S3 in parquet format

         Parameters
        ----------
        spark: spark session
        output_data : output file path in s3

    """

    # read date data file
    df = spark.sql("SELECT sequence(to_date('2015-12-01'), to_date('2022-01-01'), interval 1 day) as date").withColumn("date", explode(col("date")))

    df.createOrReplaceTempView("staging_date_table")
    df_table = spark.sql("""SELECT
                                 date              AS date,
                                 day(date)         AS day,
                                 weekofyear(date)  AS week,
                                 month(date)       AS month,
                                 year(date)        AS year,
                                 dayofweek(date)   AS weekday
                          FROM staging_date_table
                        """)

    data_null_checks(df_table,'date')
    data_unique_checks(df_table,'date')

    # write date table to parquet files
    df_table.write.partitionBy("month").parquet(output_data+"date_table.parquet")


def main():

    spark = create_spark_session()

    IMMI_DATA = config['S3']['IMMI_DATA']
    DEMO_DATA = config['S3']['DEMO_DATA']
    AIRPORT_DATA = config['S3']['AIRPORT_DATA']
    COUNTRY_DATA = config['S3']['COUNTRY_DATA']
    REGION_DATA = config['S3']['REGION_DATA']

    OUTPUT_PATH = config['S3-OUTPUT']['OUTPUT_PATH']

    process_airport_data(spark, AIRPORT_DATA, OUTPUT_PATH)
    process_country_data(spark, COUNTRY_DATA, OUTPUT_PATH)
    process_region_data(spark, REGION_DATA, OUTPUT_PATH)
    process_demographics_data(spark, DEMO_DATA, OUTPUT_PATH)
    process_immigration_data(spark, IMMI_DATA, OUTPUT_PATH)
    process_time_data(spark,OUTPUT_PATH)


if __name__ == "__main__":
    main()
