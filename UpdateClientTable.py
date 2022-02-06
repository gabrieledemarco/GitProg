import time
from datetime import datetime

import pyspark.sql.functions as F
from tqdm import tqdm

from BinanceService import BinanceService
from DbService import DbService
from SparkDB_table_service import SparkToDBService


class UpdateClientTable:

    def __init__(self):

        self.db = DbService()
        self.spark_ser = SparkToDBService()
        self.users = self.spark_ser.download_users()

    def last_update_date(self, name_table_update):

        return self.db.get_select_with_where(select_columns='update_date', name_table='update_table',
                                                    where_columns='name_table', values_column=name_table_update)

    def get_new_users(self, update_date: datetime):

        return self.users.select("id_user", "api_key", "api_secret", "registration_date")\
                    .where(F.col("registration_date") > update_date)

    def get_old_users(self, update_date: datetime):
        return self.users.select("id_user", "api_key", "api_secret", "registration_date")\
                    .where(F.col("registration_date") < update_date)

    def update_dividends(self, users, type_users: str):

        name_col = self.db.name_columns(name_table="dividends")
        update_date = self.last_update_date(name_table_update="dividends")

        all_div = []
        for user in users.rdd.collect():
            ser_bin = BinanceService(api_key=user[1], api_secret=user[2])
            if type_users == "old":
                limit_div = (datetime.now().date() - update_date.date()).days
            else:
                date_registration = self.db.get_select_with_where(select_columns="registration_date",
                                                                  name_table="users", where_columns="api_key",
                                                                  values_column=user[1])
                limit_div = (datetime.now().date() - date_registration.date()).days

            all_div_user = []
            asset_tot = self.db.get_all_value_in_column(name_column="coin", name_table="crypto")
            for asset in tqdm(asset_tot, desc="Dividends's table upsert"):
                try:
                    dividends = ser_bin.get_daily_div_history(asset=asset, limit=limit_div)
                    if dividends:
                        for dividend in dividends:
                            all_div_user.append((user[0], dividend['id'], dividend['tranId'], dividend['asset'],
                                                 dividend['amount'], dividend['divTime'], dividend['enInfo']))

                except Exception as ex:
                    if str(ex).startswith("APIError(code=-1121)"):
                        pass
                    elif str(ex).startswith("APIError(code=-1003)"):
                        time.sleep(60)
                        pass
                    else:
                        print(ex)
                        break

        all_div = sum(all_div, [])
        df_symbols = self.spark_ser.spark.createDataFrame(all_div).toDF(name_col)
        df_symbols.write.insertInto(tableName="public.dividends", overwrite=False)
        self.db.insert(name_table='update_table', list_record=["dividends", datetime.now()])

    def update_orders(self, users, type_users: str):
        name_col = self.db.name_columns(name_table="orders")
        update_date = self.last_update_date(name_table_update="orders")
