from booking_analysis import read_booking_summary , top_3_revenue
from pyspark.sql import SparkSession

spark = SparkSession.builder\
                    .appName("testing")\
                    .getOrCreate()
df = read_booking_summary(spark)
result_df =  top_3_revenue(df)

result_df.display()