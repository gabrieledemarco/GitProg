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
        end_date = datetime.now()

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

            all_div.append(all_div_user)

        all_div = sum(all_div, [])
        df_symbols = self.spark_ser.spark.createDataFrame(all_div).toDF(name_col)
        df_symbols.write.insertInto(tableName="public.dividends", overwrite=False)
        if self.db.count_records(name_table="update_table") == 3:
            self.db.insert(name_table='update_table', list_record=["dividends", end_date])
        else:
            self.db.delete_where_condition(name_table='update_table', where_columns="name_table",
                                           values_column="dividends")
            self.db.insert(name_table='update_table', list_record=["dividends", end_date])

    def update_orders(self, users, type_users: str):

        name_col = self.db.name_columns(name_table="orders")
        symbol_tot = self.db.get_all_value_in_column(name_column="symbol", name_table="symbols")
        update_date = self.last_update_date(name_table_update="orders")
        end_date = datetime.now()

        all_orders = []
        for user in users.rdd.collect():
            ser_bin = BinanceService(api_key=user[1], api_secret=user[2])
            if type_users == "old":
                start_date = update_date
            else:
                start_date = self.db.get_select_with_where(select_columns="registration_date",
                                                                  name_table="users", where_columns="api_key",
                                                                  values_column=user[1])
            start_date = int(start_date.timestamp() * 1000)
            end_date = int(end_date.timestamp() * 1000)
            all_ord_user = []
            for pair in symbol_tot:
                try:
                    orders = ser_bin.get_orders(symbol=pair, start_time=start_date, end_time=end_date)
                    if orders:
                        for order in orders:
                            all_ord_user.append((user[0], order['symbol'], order['orderId'], order['clientOrderId'],
                                                 order['price'], order['origQty'], order['executedQty'],
                                                 order['cummulativeQuoteQty'], order['status'], order['timeInForce'],
                                                 order['type'], order['side'], order['stopPrice'], order['icebergQty'],
                                                 order['time'], order['updateTime'], order['isWorking'],
                                                 order['origQuoteOrderQty']))

                except Exception as ex:
                    if str(ex).startswith("APIError(code=-1121)"):
                        pass
                    elif str(ex).startswith("APIError(code=-1003)"):
                        time.sleep(60)
                        pass
                    else:
                        print(ex)
                        break

            all_orders.append(all_ord_user)

        all_orders = sum(all_orders, [])
        df_symbols = self.spark_ser.spark.createDataFrame(all_orders).toDF(name_col)
        df_symbols.write.insertInto(tableName="public.orders", overwrite=False)
        if self.db.count_records(name_table="update_table") == 4:
            self.db.insert(name_table='update_table', list_record=["orders", end_date])
        else:
            self.db.delete_where_condition(name_table='update_table', where_columns="name_table",
                                           values_column="orders")
            self.db.insert(name_table='update_table', list_record=["orders", end_date])

    def update_trades(self, users, type_users: str):

        name_col = self.db.name_columns(name_table="trades")
        symbol_orders = self.db.get_all_value_in_column(name_column="symbol", name_table="orders")
        update_date = self.last_update_date(name_table_update="trades")
        end_date = datetime.now()

        all_trade = []
        for user in users.rdd.collect():
            ser_bin = BinanceService(api_key=user[1], api_secret=user[2])
            if type_users == "old":
                start_date = update_date
            else:
                start_date = self.db.get_select_with_where(select_columns="registration_date",
                                                           name_table="users", where_columns="api_key",
                                                           values_column=user[1])
            start_date = int(start_date.timestamp() * 1000)
            end_date = int(end_date.timestamp() * 1000)

            all_trade_user = []
            for pair in symbol_orders:
                try:
                    trades = ser_bin.get_trades(symbol=pair[0], start_time=start_date, end_time=end_date)
                    for trade in trades:
                        all_trade_user.append((user[0], trade['symbol'], trade['id'], trade['orderId'], trade['price'],
                                               trade['qty'], trade['quoteQty'], trade['commission'],
                                               trade['commissionAsset'], trade['time'], trade['isBuyer'],
                                               trade['isMaker'], trade['isBestMatch']))

                except Exception as ex:
                    if str(ex).startswith("APIError(code=-1121)"):
                        pass
                    elif str(ex).startswith("APIError(code=-1003)"):
                        time.sleep(60)
                        pass
                    else:
                        print(ex)
                        break

            all_trade.append(all_trade_user)
        all_trade = sum(all_trade, [])
        df_trades = self.spark_ser.spark.createDataFrame(all_trade).toDF(name_col)
        df_trades.write.insertInto(tableName="public.trades", overwrite=False)
        if self.db.count_records(name_table="update_table") == 5:
            self.db.insert(name_table='update_table', list_record=["trades", end_date])
        else:
            self.db.delete_where_condition(name_table='update_table', where_columns="name_table",
                                           values_column="trades")
            self.db.insert(name_table='update_table', list_record=["trades", end_date])

