import configparser
import os

class config_reader:

    def get_config_pipeline(self):
        parser = configparser.ConfigParser()
        parser.read("D:\DataEngineering_Projects\SCD_TYPE_2\config\pipeline.ini")
        return parser

    def getSparkConfig(self):
        spark_config ={
            "spark.sql.autoBroadcastJoinThreshold":"-1",
            "spark.port.maxRetries":"100",
            "spark.sql.sources.partitionColumnTypeInference.enabled":"false",
            "spark.shuffle.partitions":"10"
        }
        return spark_config
