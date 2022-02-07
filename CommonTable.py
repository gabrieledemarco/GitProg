from datetime import datetime

from tqdm import tqdm

from BinanceService import BinanceService
from DbService import DbService
from InsertValueInTable import InsertValueInTable


class CommonTable: # Update_csn (Crypto, Symbols, Networks)

    def __init__(self):
        self.db = DbService()
        self.api = self.db.get_select_with_where(select_columns=['api_key', 'api_secret'], name_table="users",
                                                     where_columns='id_user',
                                                     values_column=1)
        self.ser_bin = BinanceService(api_key=self.api[0], api_secret=self.api[1])
        self.ins_tab = InsertValueInTable(api_key=self.api[0], api_secret=self.api[1])

    def first_insert_common_table(self):
        self.ins_tab.insert_Crypto()
        self.ins_tab.insert_symbols()
        self.ins_tab.insert_networks()

    def update_crypto(self):
        crypto_db = self.db.get_all_value_in_column(name_column='coin', name_table='crypto')
        coins_bin = self.ser_bin.get_coins()
        crypto_to_add = list(set(coins_bin) - set(crypto_db))
        crypto_to_del = list(set(crypto_db) - set(coins_bin))

        if crypto_to_del:
            for crypto in crypto_to_del:
                self.db.delete_where_condition(name_table='coin', where_columns='coin', values_column=crypto)

        add_list = []
        if crypto_to_add:
            for coin in tqdm(coins_bin, desc="Crypto's table upsert"):
                if coin['coin'] in crypto_to_add:
                    add_list.append((coin['coin'], coin['name'], coin['withdrawAllEnable'], coin['trading'],
                                     coin['networkList'][0]['withdrawEnable']))

        self.db.insert(name_table="crypto", list_record=add_list)
        if self.db.is_not_empty('update_table'):
            self.db.insert(name_table='update_table', list_record=["update_table", datetime.now()])
        else:
            self.db.delete_where_condition(name_table='update_table', where_columns="name_table",
                                           values_column="crypto")
            self.db.insert(name_table='update_table', list_record=["crypto", datetime.now()])

    def update_symbols(self):
        symbols_db = self.db.get_all_value_in_column(name_column='symbol', name_table='symbols')
        symbol_data = self.ser_bin.get_symbols()
        symbols_bin = [symbol['symbol'] for symbol in symbol_data]

        symbols_to_add = list(set(symbols_bin) - set(symbols_db))
        symbols_to_del = list(set(symbols_db) - set(symbols_bin))

        if symbols_to_del:
            for symbol in symbols_to_del:
                self.db.delete_where_condition(name_table='symbols', where_columns='symbol', values_column=symbol)

        add_symbols = []
        if symbols_to_add:
            for i in range(len(symbols_bin)):
                if symbols_bin[i] in symbols_to_add:
                    add_symbols.append((symbol_data[i]['symbol'], symbol_data[i]['baseAsset'],
                                        symbol_data[i]['quoteAsset']))

        self.db.insert(name_table='symbols', list_record=add_symbols)
        if self.db.count_records(name_table="update_table") == 1:
            self.db.insert(name_table='update_table', list_record=["symbols", datetime.now()])
        else:
            self.db.delete_where_condition(name_table='update_table', where_columns="name_table",
                                           values_column="symbols")
            self.db.insert(name_table='update_table', list_record=["symbols", datetime.now()])

    # def update_networks(self):
    # networks devo pensarci
















