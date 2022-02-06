from Spark_to_DB import SparkToDB


class SparkToDBService:

    def __init__(self):
        self.conn = SparkToDB(path="~/Desktop/GitProg/", app_name="Project")
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

    def update_dividends(self):
        users = self.download_users()
        users.select('id_user', 'api_key', 'api_secret')


a = SparkToDBService()
a.download_users().show()
