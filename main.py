from config_postgres_alchemy import postgres_sql as settings
import findspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import os
import warnings

conf = SparkConf().setAll(pairs=[("spark.jars", "C:\SqlJar/postgresql-42.3.2.jar"),
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
        .appName("ok")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.warehouse.dir", os.getcwd())
        .config(conf=conf)
        .getOrCreate()
)

sc = spark.sparkContext
sqlContext = SQLContext(sc)

"""
df1 = spark.read.format("jdbc").options(driver="org.postgresql.Driver",
                                            url=f"jdbc:postgresql://{settings['host']}:{settings['port']}"
                                                f"/{settings['db']}") \
                                            .option("user", "postgres") \
                                            .option("password", "gabriele") \
                                            .options(dbtable="public.trades").load()
df1.show()

"""
