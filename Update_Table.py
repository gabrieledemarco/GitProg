from spark_connection import SparkConnection


class UpdateTable:

    def __init__(self):
        self.conn = SparkConnection(path="~/Desktop/GitProg/", app_name="Project")
        self.spark = self.conn.connection_spark()

    def orders_table(self):
        return self.conn.load_table(spark=self.spark, schema="public", name_table="orders")

    def trades_table(self):
        return self.conn.load_table(spark=self.spark, schema="public", name_table="trades")

    def dividends_table(self):
        return self.conn.load_table(spark=self.spark, schema="public", name_table="dividends")

