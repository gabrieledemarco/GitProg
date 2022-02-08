import time
from datetime import datetime

import pyspark.sql.functions as F
from tqdm import tqdm

import DateFunction as dT
import TransfromDataBinance as tdb
from BinanceService import BinanceService
from CommonTable import CommonTable
from DbService import DbService
from SparkDB_table_service import SparkToDBService


class UpdateClientTable:

    def __init__(self):

        self.db = DbService()
        self.spark_ser = SparkToDBService()
        self.users = self.spark_ser.download_data_table(name_table="users")
        self.common = CommonTable()

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

        update_date = self.last_update_date(name_table_update="dividends")
        end_date = dT.now_date()

        all_div = []
        for user in users.rdd.collect():
            ser_bin = BinanceService(api_key=user[1], api_secret=user[2])
            if type_users == "old":
                limit_div = dT.limit(start_date=update_date, end_date=end_date)
            else:
                date_registration = self.db.get_select_with_where(select_columns="registration_date",
                                                                  name_table="users", where_columns="api_key",
                                                                  values_column=user[1])

                limit_div = dT.limit(start_date=date_registration, end_date=end_date)

            asset_tot = self.db.get_all_value_in_column(name_column="coin", name_table="crypto")
            for asset in tqdm(asset_tot, desc="Dividends's table upsert"):
                try:
                    dividends = ser_bin.get_daily_div_history(asset=asset, limit=limit_div)
                    if dividends:
                        for dividend in dividends:
                            tuple_div = tdb.get_tuple_dividends(id_user=user[0], dividend=dividend)
                            all_div.append(tuple_div)

                except Exception as ex:
                    if str(ex).startswith("APIError(code=-1121)"):
                        pass
                    elif str(ex).startswith("APIError(code=-1003)"):
                        time.sleep(60)
                        pass
                    else:
                        print(ex)
                        break

        self.spark_ser.insert_in_table(name_table="dividends", input_data=all_div)
        self.common.update_update_table(name_table="dividends", end_date=end_date)

    def update_orders(self, users, type_users: str):

        symbol_tot = self.db.get_all_value_in_column(name_column="symbol", name_table="symbols")
        update_date = self.last_update_date(name_table_update="orders")
        end_date = dT.now_date()

        all_orders = []
        for user in users.rdd.collect():
            ser_bin = BinanceService(api_key=user[1], api_secret=user[2])
            if type_users == "old":
                start_date = update_date
            else:
                start_date = self.db.get_select_with_where(select_columns="registration_date",
                                                                  name_table="users", where_columns="api_key",
                                                                  values_column=user[1])

            start_date = dT.datetime_to_milliseconds_int(start_date)
            end_date = dT.datetime_to_milliseconds_int(end_date)

            for pair in symbol_tot:
                try:
                    orders = ser_bin.get_orders(symbol=pair, start_time=start_date, end_time=end_date)
                    if orders:
                        for order in orders:
                            tuple_orders = tdb.get_tuple_orders(id_user=user[0], order=order)
                            all_orders.append(tuple_orders)

                except Exception as ex:
                    if str(ex).startswith("APIError(code=-1121)"):
                        pass
                    elif str(ex).startswith("APIError(code=-1003)"):
                        time.sleep(60)
                        pass
                    else:
                        print(ex)
                        break

        self.spark_ser.insert_in_table(name_table="orders", input_data=all_orders)

        self.common.update_update_table(name_table="orders", end_date=end_date)

    def update_trades(self, users, type_users: str):

        symbol_orders = self.db.get_all_value_in_column(name_column="symbol", name_table="orders")
        update_date = self.last_update_date(name_table_update="trades")
        end_date = dT.now_date()

        all_trade = []
        for user in users.rdd.collect():
            ser_bin = BinanceService(api_key=user[1], api_secret=user[2])
            if type_users == "old":
                start_date = update_date
            else:
                start_date = self.db.get_select_with_where(select_columns="registration_date",
                                                           name_table="users", where_columns="api_key",
                                                           values_column=user[1])
            start_date = dT.datetime_to_milliseconds_int(start_date)
            end_date = dT.datetime_to_milliseconds_int(end_date)

            for pair in symbol_orders:
                try:
                    trades = ser_bin.get_trades(symbol=pair[0], start_time=start_date, end_time=end_date)
                    for trade in trades:
                        tuple_trade = tdb.get_tuple_trades(id_user=user[0], trade=trade)
                        all_trade.append(tuple_trade)

                except Exception as ex:
                    if str(ex).startswith("APIError(code=-1121)"):
                        pass
                    elif str(ex).startswith("APIError(code=-1003)"):
                        time.sleep(60)
                        pass
                    else:
                        print(ex)
                        break

        self.spark_ser.insert_in_table(name_table="trades", input_data=all_trade)

        self.common.update_update_table(name_table="trades", end_date=end_date)

    def deposit_crypto(self, users, type_users: str):

        update_date = self.last_update_date(name_table_update="deposits_crypto")
        end_date = dT.now_date()

        all_deposit = []
        for user in users.rdd.collect():
            ser_bin = BinanceService(api_key=user[1], api_secret=user[2])
            if type_users == "old":
                start_date = update_date
            else:
                start_date = self.db.get_select_with_where(select_columns="registration_date",
                                                           name_table="users", where_columns="api_key",
                                                           values_column=user[1])
            start_date = dT.datetime_to_milliseconds_int(start_date)
            end_date = dT.datetime_to_milliseconds_int(end_date)

            deposits = ser_bin.get_deposit_crypto(start_date=start_date, end_date=end_date)
            if deposits:
                for deposit in deposits:
                    tuple_deposit = tdb.get_tuple_deposit_crypto(id_user=user[0], dep=deposit)
                    all_deposit.append(tuple_deposit)

        self.spark_ser.insert_in_table(name_table="deposits_crypto", input_data=all_deposit)
        self.common.update_update_table(name_table="deposits_crypto", end_date=end_date)

    def withdraw_crypto(self, users, type_users: str):
        update_date = self.last_update_date(name_table_update="withdraw_crypto")
        end_date = dT.now_date()

        all_withdraw = []
        for user in users.rdd.collect():
            ser_bin = BinanceService(api_key=user[1], api_secret=user[2])
            if type_users == "old":
                start_date = update_date
            else:
                start_date = self.db.get_select_with_where(select_columns="registration_date",
                                                           name_table="users", where_columns="api_key",
                                                           values_column=user[1])
            start_date = dT.datetime_to_milliseconds_int(start_date)
            end_date = dT.datetime_to_milliseconds_int(end_date)

            withdraw_crypto = ser_bin.get_withdraw_crypto(start_date=start_date, end_date=end_date)

            if withdraw_crypto:
                for withdraw in withdraw_crypto:
                    if 'confirmNo' in withdraw:
                        tuple_withdraw = tdb.get_tuple_withdraw_crypto(id_user=user[0], withdraw=withdraw)
                        all_withdraw.append(tuple_withdraw)

        self.spark_ser.insert_in_table(name_table="withdraw_crypto", input_data=all_withdraw)
        self.common.update_update_table(name_table="withdraw_crypto", end_date=end_date)

    def deposit_withdraw_fiat(self, users, type_users: str, withdraws_deposits: str):

        update_date = self.last_update_date(name_table_update="deposit_withdraw_fiat")
        end_date = dT.now_date()

        all_fiat = []
        for user in users.rdd.collect():
            ser_bin = BinanceService(api_key=user[1], api_secret=user[2])
            if type_users == "old":
                start_date = update_date
            else:
                start_date = self.db.get_select_with_where(select_columns="registration_date",
                                                           name_table="users", where_columns="api_key",
                                                           values_column=user[1])
            start_date = dT.datetime_to_milliseconds_int(start_date)
            end_date = dT.datetime_to_milliseconds_int(end_date)

            if withdraws_deposits == "deposit":
                dep_fiat = ser_bin.get_deposit_fiat(start_date=start_date, end_date=end_date)
                if len(dep_fiat['data']) > 0:
                    if 'data' in dep_fiat:
                        for deposit in dep_fiat['data']:
                            tuple_deposits = tdb.get_tuple_deposit_withdraw_fiat(id_user=user[0], dep=deposit,
                                                                                 tran_type="D")
                            all_fiat.append(tuple_deposits)

            else:
                withdraw_fiat = ser_bin.get_withdraw_fiat(start_date=start_date, end_date=end_date)
                if len(withdraw_fiat['data']) > 0:
                    if 'data' in withdraw_fiat:
                        for withdraw in withdraw_fiat['data']:
                            tuple_withdraws = tdb.get_tuple_deposit_withdraw_fiat(id_user=user[0], dep=withdraw,
                                                                                  tran_type="W")
                            all_fiat.append(tuple_withdraws)

        self.spark_ser.insert_in_table(name_table="deposit_withdraw_fiat", input_data=all_fiat)
        self.common.update_update_table(name_table="deposit_withdraw_fiat", end_date=end_date)

    def buy_sell_fiat(self, users, type_users: str, buy_sell: str):
        update_date = self.last_update_date(name_table_update="deposit_withdraw_fiat")
        end_date = dT.now_date()
        all_transaction = []
        for user in users.rdd.collect():
            ser_bin = BinanceService(api_key=user[1], api_secret=user[2])
            if type_users == "old":
                start_date = update_date
            else:
                start_date = self.db.get_select_with_where(select_columns="registration_date",
                                                           name_table="users", where_columns="api_key",
                                                           values_column=user[1])
            start_date = dT.datetime_to_milliseconds_int(start_date)
            end_date = dT.datetime_to_milliseconds_int(end_date)

            if buy_sell == "buy":
                purchase_cx_fiat = ser_bin.get_purchase_cx_fiat(start_date=start_date, end_date=end_date)
                if len(purchase_cx_fiat['data']) > 0:
                    for purchase in purchase_cx_fiat['data']:
                        tuple_transaction = tdb.get_tuple_buy_sell_fiat(id_user=user[0], buy_sell=purchase,
                                                                        tran_type="B")
                        all_transaction.append(tuple_transaction)

            else:
                sell_cx_fiat = ser_bin.get_sell_cx_fiat(start_date=start_date, end_date=end_date)
                if 'data' in sell_cx_fiat:
                    if len(sell_cx_fiat['data']) > 0:
                        for sell in sell_cx_fiat['data']:
                            tuple_sell = tdb.get_tuple_buy_sell_fiat(id_user=user[0], buy_sell=sell, tran_type="S")
                            all_transaction.append(tuple_sell)

            self.spark_ser.insert_in_table(name_table="buy_sell_fiat", input_data=all_transaction)
            self.common.update_update_table(name_table="buy_sell_fiat", end_date=end_date)





