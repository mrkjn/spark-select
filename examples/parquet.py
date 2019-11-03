from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    # create SparkSession
    spark = SparkSession.builder \
        .master("local") \
        .appName("spark-select in python") \
        .getOrCreate()

    # filtered schema
    st = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), False),
    ])

    df = spark \
        .read \
        .format('minioSelectParquet') \
        .schema(st) \
        .load("cos://testbucket/people.parquet")

    # show all rows.
    df.show()

    # show only filtered rows.
    df.select("*").filter("age > 19").show()
