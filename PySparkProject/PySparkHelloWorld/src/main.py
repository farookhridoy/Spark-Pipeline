## Note that Spark 2.4 needs Java 8 to run. Its version of PySpark is also only compatible with Python 3.7 (or earlier).
## Spark 3 works with Java 11 (and also Java 8). Its version of PySaprk is compatible with Python 3.7 (and newer).

## pip install pyspark findspark

import findspark
from pyspark.sql.functions import upper

findspark.init()

from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql import Row

def init_spark():
    spark = SparkSession.builder\
        .master("local[1]")\
        .appName("HelloWorld")\
        .config("spark.driver.cores", 2) \
        .config("spark.sql.shuffle.partitions", 10)\
        .getOrCreate()
    sc = spark
    return spark, sc

def main():
    spark, sc = init_spark()
    nums = sc.sparkContext.parallelize([1, 2, 3, 4])
    print(nums.map(lambda x: x*x).collect())
    print("APP Name :" + sc.sparkContext.appName)
    print("Master :" + sc.sparkContext.master)
    print("Version :" + sc.sparkContext.version)


def df():
    spark, sc = init_spark()
    dataf = sc.createDataFrame([
        Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2023, 1, 1, 12, 0)),
        Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2013, 1, 2, 12, 0)),
        Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
    ])
    dataf.createOrReplaceTempView("population")
    print(dataf.show())
    print(dataf.printSchema())
    print(dataf.select("a", "b", "c").describe().show())
    print(dataf.withColumn('upper_c', upper(dataf.c)).show())

    datafile2 = sc.sql("SELECT a as Id, b as Point from population order by e asc")
    print(datafile2.show())


if __name__ == '__main__':
    #main()
    df()