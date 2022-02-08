import os
import warnings

import findspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

from DbService import DbService
from config_postgres_alchemy import postgres_sql as settings


class SparkToDB:

    def __init__(self, path, app_name):
        self.settings = settings    # PostgresSQL on ElephantSQL conncetion settings
        self.path = path            # Directory path for postgresqk-42.3.2.jar driver
        self.app_name = app_name    # app name
        self.db = DbService()

    def connection_spark(self):
        """
        Spark Connection to Postgress DB
        :return:
        """
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

    def load_table(self, spark, name_table):
        """
                Desc: Download Postgress Table into Spark RDD
        :param spark: Spark Connection
        :param name_table: Table name
        :return:
        """
        data = spark.read.format("jdbc").options(driver="org.postgresql.Driver",
                                                 url=f"jdbc:postgresql://{self.settings['host']}:"
                                                     f"{self.settings['port']}/{self.settings['db']}") \
                                                .option("user", self.settings['user']) \
                                                .option("password", self.settings['password']) \
                                                .options(dbtable=f"public.{name_table}").load()
        return data

    def insert_in_table(self, spark, name_table: str, input_data: list):
        name_cols = self.db.name_columns(name_table=name_table)
        df_symbols = spark.createDataFrame(input_data).toDF(name_cols)
        df_symbols.write.insertInto(tableName=f"public.{name_table}", overwrite=False)
