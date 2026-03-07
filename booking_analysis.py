
def read_booking_summary(spark):
    schema = "booked_by string , booked_date date , revenue double"

    df = (
            spark.read.format("csv")
            .option("header" , "true")
            .schema(schema)
            .load(path  = '/Volumes/dev/spark_db/datasets/spark_programming/data/booking-summary.csv')
    )

    return df

def top_3_revenue(df):
    from pyspark.sql.window import Window
    from pyspark.sql.functions import dense_rank , col

    result_df = (
        df.withColumn("rnk" , dense_rank().over(
                                                Window.partitionBy("booked_by")
                                                .orderBy(col("revenue").desc_nulls_last())
                                                .rowsBetween(Window.unboundedPreceding , Window.currentRow)
        )).where("rnk < 4").drop("rnk")
    )
    return result_df