from spark_connection import SparkConnection


class UpdateTable:

    def __init__(self):
        self.conn = SparkConnection(path="~/Desktop/GitProg/", app_name="Project")
        self.spark = self.conn.connection_spark()

    def download_orders(self):
        return self.conn.load_table(spark=self.spark, schema="public", name_table="orders")

    def download_trades(self):
        return self.conn.load_table(spark=self.spark, schema="public", name_table="trades")

    def download_dividends(self):
        return self.conn.load_table(spark=self.spark, schema="public", name_table="dividends")

    def download_buy_sell_fiat(self):
        return self.conn.load_table(spark=self.spark, schema="public", name_table="buy_sell_fiat")

    def download_deposit_withdraw_fiat(self):
        return self.conn.load_table(spark=self.spark, schema="public", name_table="deposit_withdraw_fiat")

    def download_deposits_crypto(self):
        return self.conn.load_table(spark=self.spark, schema="public", name_table="deposits_crypto")

    def download_users(self):
        return self.conn.load_table(spark=self.spark, schema="public", name_table="users")

    def download_withdraw_crypto(self):
        return self.conn.load_table(spark=self.spark, schema="public", name_table="withdraw_crypto")
