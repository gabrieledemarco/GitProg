import os
import warnings

import findspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

from config_postgres_alchemy import postgres_sql as settings


class SparkToDB:

    def __init__(self, path, app_name):
        self.settings = settings
        self.path = path
        self.app_name = app_name

    def connection_spark(self):
        conf = SparkConf().setAll(pairs=[("spark.jars", f"{self.path}/postgresql-42.3.2.jar"),
                                         ("spark.jars.packages", "org.postgresql:postgresql:42.3.2")])

        try:

            findspark.init()
        except ImportError:
            pass

        warnings.filterwarnings('ignore')

        spark = (
            SparkSession
            .builder
            .master("local[*]")
            .appName(self.app_name)
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.warehouse.dir", os.getcwd())
            .config(conf=conf)
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")
        return spark

    def load_table(self, spark, schema, name_table):
        data = spark.read.format("jdbc").options(driver="org.postgresql.Driver",
                                                 url=f"jdbc:postgresql://{self.settings['host']}:"
                                                     f"{self.settings['port']}/{self.settings['db']}") \
                                                .option("user", self.settings['user']) \
                                                .option("password", self.settings['password']) \
                                                .options(dbtable=f"{schema}.{name_table}").load()
        return data
