import time

import pandas as pd

import DateFunction as dT
import TransfromDataBinance as tdb
from BinanceService import BinanceService
from CommonTable import CommonTable
from CreateTables import url
from DbService import DbService


class UpdateClientTable:

    def __init__(self):

        self.db = DbService()
        self.df = pd.read_sql_table('users', url)
        self.common = CommonTable()

    def last_update_date(self, name_table_update):

        return self.db.get_select_with_where(select_columns='update_date', name_table='update_table',
                                                    where_columns='name_table', values_column=name_table_update)

    def update_dividends(self):

        update_date = self.last_update_date(name_table_update="dividends")
        end_date = dT.now_date()

        all_div = []
        for index, row in self.df.iterrows():
            ser_bin = BinanceService(api_key=row['api_key'], api_secret=row['api_secret'])
            if row['date_registration'] < update_date:

                limit_div = dT.limit(start_date=update_date, end_date=end_date)
            else:
                limit_div = dT.limit(start_date=row['date_registration'], end_date=end_date)

            asset_tot = self.db.get_all_value_in_column(name_column="coin", name_table="crypto")
            for asset in asset_tot:
                try:
                    dividends = ser_bin.get_daily_div_history(asset=asset, limit=limit_div)
                    if dividends:
                        for dividend in dividends:
                            tuple_div = tdb.get_tuple_dividends(id_user=row['id_user'], dividend=dividend)
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

        if all_div:
            self.db.insert(name_table="dividends", list_record=all_div)

        self.common.update_update_table(name_table="dividends", end_date=end_date)

    def update_orders(self):

        symbol_tot = self.db.get_all_value_in_column(name_column="symbol", name_table="symbols")
        update_date = self.last_update_date(name_table_update="orders")
        end_date = dT.now_date()

        all_orders = []
        for index, row in self.df.iterrows():
            ser_bin = BinanceService(api_key=row['api_key'], api_secret=row['api_secret'])
            if row['date_registration'] < update_date:
                start_date = update_date
            else:
                start_date = row['registration_date']

            start_date = dT.datetime_to_milliseconds_int(input_data=start_date)
            end_date = dT.datetime_to_milliseconds_int(input_data=end_date)

            for pair in symbol_tot:
                try:
                    orders = ser_bin.get_orders(symbol=pair, start_time=start_date, end_time=end_date)
                    if orders:
                        for order in orders:
                            tuple_orders = tdb.get_tuple_orders(id_user=row['id_user'], order=order)
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

        if all_orders:
            self.db.insert(name_table="orders", list_record=all_orders)
        self.common.update_update_table(name_table="orders", end_date=end_date)

    def update_trades(self):

        symbol_orders = self.db.get_all_value_in_column(name_column="symbol", name_table="orders")
        update_date = self.last_update_date(name_table_update="trades")
        end_date = dT.now_date()

        all_trade = []
        for index, row in self.df.iterrows():
            ser_bin = BinanceService(api_key=row['api_key'], api_secret=row['api_secret'])
            if row['date_registration'] < update_date:
                start_date = update_date
            else:
                start_date = row['registration_date']
            start_date = dT.datetime_to_milliseconds_int(input_data=start_date)
            end_date = dT.datetime_to_milliseconds_int(input_data=end_date)

            for pair in symbol_orders:
                try:
                    trades = ser_bin.get_trades(symbol=pair[0], start_time=start_date, end_time=end_date)
                    for trade in trades:
                        tuple_trade = tdb.get_tuple_trades(id_user=row['id_user'], trade=trade)
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
        if all_trade:
            self.db.insert(name_table="trades", list_record=all_trade)

        self.common.update_update_table(name_table="trades", end_date=end_date)

    def deposit_crypto(self):

        update_date = self.last_update_date(name_table_update="deposits_crypto")
        end_date = dT.now_date()

        all_deposit = []
        for index, row in self.df.iterrows():
            ser_bin = BinanceService(api_key=row['api_key'], api_secret=row['api_secret'])
            if row['date_registration'] < update_date:
                start_date = update_date
            else:
                start_date = row['registration_date']

            start_date = dT.datetime_to_milliseconds_int(input_data=start_date)
            end_date = dT.datetime_to_milliseconds_int(input_data=end_date)

            deposits = ser_bin.get_deposit_crypto(start_date=start_date, end_date=end_date)
            if deposits:
                for deposit in deposits:
                    tuple_deposit = tdb.get_tuple_deposit_crypto(id_user=row['id_user'], dep=deposit)
                    all_deposit.append(tuple_deposit)

        if all_deposit:
            self.db.insert(name_table="deposits_crypto", list_record=all_deposit)

        self.common.update_update_table(name_table="deposits_crypto", end_date=end_date)

    def withdraw_crypto(self):
        update_date = self.last_update_date(name_table_update="withdraw_crypto")
        end_date = dT.now_date()

        all_withdraw = []
        for index, row in self.df.iterrows():
            ser_bin = BinanceService(api_key=row['api_key'], api_secret=row['api_secret'])
            if row['date_registration'] < update_date:
                start_date = update_date
            else:
                start_date = row['registration_date']

            start_date = dT.datetime_to_milliseconds_int(input_data=start_date)
            end_date = dT.datetime_to_milliseconds_int(input_data=end_date)

            withdraw_crypto = ser_bin.get_withdraw_crypto(start_date=start_date, end_date=end_date)

            if withdraw_crypto:
                for withdraw in withdraw_crypto:
                    if 'confirmNo' in withdraw:
                        tuple_withdraw = tdb.get_tuple_withdraw_crypto(id_user=row['id_user'], withdraw=withdraw)
                        all_withdraw.append(tuple_withdraw)

        if all_withdraw:
            self.db.insert(name_table="withdraw_crypto", list_record=all_withdraw)

        self.common.update_update_table(name_table="withdraw_crypto", end_date=end_date)

    def deposit_withdraw_fiat(self, withdraws_deposits: str):

        update_date = self.last_update_date(name_table_update="deposit_withdraw_fiat")
        end_date = dT.now_date()

        all_fiat = []
        for index, row in self.df.iterrows():
            ser_bin = BinanceService(api_key=row['api_key'], api_secret=row['api_secret'])
            if row['date_registration'] < update_date:
                start_date = update_date
            else:
                start_date = row['registration_date']

            start_date = dT.datetime_to_milliseconds_int(input_data=start_date)
            end_date = dT.datetime_to_milliseconds_int(input_data=end_date)

            if withdraws_deposits == "deposit":
                dep_fiat = ser_bin.get_deposit_fiat(start_date=start_date, end_date=end_date)
                if len(dep_fiat['data']) > 0:
                    if 'data' in dep_fiat:
                        for deposit in dep_fiat['data']:
                            tuple_deposits = tdb.get_tuple_deposit_withdraw_fiat(id_user=row['id_user'], dep=deposit,
                                                                                 tran_type="D")
                            all_fiat.append(tuple_deposits)

            else:
                withdraw_fiat = ser_bin.get_withdraw_fiat(start_date=start_date, end_date=end_date)
                if len(withdraw_fiat['data']) > 0:
                    if 'data' in withdraw_fiat:
                        for withdraw in withdraw_fiat['data']:
                            tuple_withdraws = tdb.get_tuple_deposit_withdraw_fiat(id_user=row['id_user'], dep=withdraw,
                                                                                  tran_type="W")
                            all_fiat.append(tuple_withdraws)

        if all_fiat:
            self.db.insert(name_table="deposit_withdraw_fiat", list_record=all_fiat)

        self.common.update_update_table(name_table="deposit_withdraw_fiat", end_date=end_date)

    def buy_sell_fiat(self, buy_sell: str):
        update_date = self.last_update_date(name_table_update="deposit_withdraw_fiat")
        end_date = dT.now_date()

        all_transaction = []
        for index, row in self.df.iterrows():
            ser_bin = BinanceService(api_key=row['api_key'], api_secret=row['api_secret'])
            if row['date_registration'] < update_date:
                start_date = update_date
            else:
                start_date = row['registration_date']

            start_date = dT.datetime_to_milliseconds_int(input_data=start_date)
            end_date = dT.datetime_to_milliseconds_int(input_data=end_date)

            if buy_sell == "buy":
                purchase_cx_fiat = ser_bin.get_purchase_cx_fiat(start_date=start_date, end_date=end_date)
                if len(purchase_cx_fiat['data']) > 0:
                    for purchase in purchase_cx_fiat['data']:
                        tuple_transaction = tdb.get_tuple_buy_sell_fiat(id_user=row['id_user'], buy_sell=purchase,
                                                                        tran_type="B")
                        all_transaction.append(tuple_transaction)

            else:
                sell_cx_fiat = ser_bin.get_sell_cx_fiat(start_date=start_date, end_date=end_date)
                if 'data' in sell_cx_fiat:
                    if len(sell_cx_fiat['data']) > 0:
                        for sell in sell_cx_fiat['data']:
                            tuple_sell = tdb.get_tuple_buy_sell_fiat(id_user=row['id_user'], buy_sell=sell,
                                                                     tran_type="S")
                            all_transaction.append(tuple_sell)

            if all_transaction:
                self.db.insert(name_table="buy_sell_fiat", list_record=all_transaction)

            self.common.update_update_table(name_table="buy_sell_fiat", end_date=end_date)
