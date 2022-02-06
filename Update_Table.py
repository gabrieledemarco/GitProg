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

    def buy_sell_fiat_table(self):
        return self.conn.load_table(spark=self.spark, schema="public", name_table="buy_sell_fiat")

    def deposit_withdraw_fiat_table(self):
        return self.conn.load_table(spark=self.spark, schema="public", name_table="deposit_withdraw_fiat")

    def deposits_crypto_table(self):
        return self.conn.load_table(spark=self.spark, schema="public", name_table="deposits_crypto")

    def users_table(self):
        return self.conn.load_table(spark=self.spark, schema="public", name_table="users")

    def withdraw_crypto_table(self):
        return self.conn.load_table(spark=self.spark, schema="public", name_table="withdraw_crypto")
