from Spark_to_DB import SparkToDB


class SparkToDBService:
    """
    This class is used as a service of Spark_to_DB class
    in order to download/insert values from/into ElephantSql PostgreSql DB
    """

    def __init__(self):
        self.conn = SparkToDB(path="~/Desktop/GitProg/", app_name="Project")
        self.spark = self.conn.connection_spark()

    # Download Tables
    # --
    def download_data_table(self, name_table: str):
        return self.conn.load_table(spark=self.spark, name_table=name_table)

    def insert_in_table(self, name_table, input_data: list):
        return self.conn.insert_in_table(spark=self.spark, name_table=name_table, input_data=input_data)

    """

    def download_trades(self):
        return self.conn.load_table(spark=self.spark, name_table="trades")

    def download_dividends(self):
        return self.conn.load_table(spark=self.spark, name_table="dividends")

    def download_buy_sell_fiat(self):
        return self.conn.load_table(spark=self.spark, name_table="buy_sell_fiat")

    def download_deposit_withdraw_fiat(self):
        return self.conn.load_table(spark=self.spark, name_table="deposit_withdraw_fiat")

    def download_deposits_crypto(self):
        return self.conn.load_table(spark=self.spark, name_table="deposits_crypto")

    def download_users(self):
        return self.conn.load_table(spark=self.spark, name_table="users")

    def download_withdraw_crypto(self):
        return self.conn.load_table(spark=self.spark, name_table="withdraw_crypto")

    def insert_in_table(self, name_table, input_data: list):
        return self.conn.insert_in_table(spark=self.spark, name_table=name_table, input_data=input_data)
"""