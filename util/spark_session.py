from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


class get_spark:

    def getSparkSession(self, sparkConfig):
        app_Name = "SCD TYPE 2"
        conf = SparkConf().setAll(sparkConfig.items())
        spark = SparkSession.builder.appName(app_Name).config(conf=conf).getOrCreate()
        return spark