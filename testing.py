from pytest
import pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import SparkSession
from booking_analysis import read_booking_summary,top_3_revenue

@pytest.fixture("session")
def spark():
    return SparkSession.builder.getOrCreate()

def test_read_booking_summary(spark):
    summary_df = read_booking_summary(spark)
    loaded_records = summary_df.count()
    assert loaded_records == 58

def test_top_3_revenue(spark):
    summary_df = read_booking_summary(spark)
    result_df = top_3_revenue(summary_df)

    file_schema  == 'booked_by string , booking_date date , revenue double'
    expected_result = spark.read.format("csv")\
                                .option("header" , "true")\
                                .schema(schema)\
                                .load(path = '/Volumes/dev/spark_db/datasets/spark_programming/data/top-3-days-test-data.csv')
                                
    assertDataFrameEqual(expected_result , result_df)
    