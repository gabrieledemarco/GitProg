from datetime import datetime

from BinanceDAO import BinanceDAO


class BinanceService:

    def __init__(self, api_key: str, api_secret: str):
        self.__dao = BinanceDAO(api_key=api_key, api_secret=api_secret)

    def get_coins(self):
        return self.__dao.get_coins()

    def get_symbols(self):
        return self.__dao.get_symbols()

    def get_orders(self, symbol: str, start_time: int = None, end_time: int = None):
        return self.__dao.get_orders(symbol=symbol, start_time=start_time, end_time=end_time)

    def get_trades(self, symbol: str, start_time: int = None, end_time: int = None):
        return self.__dao.get_trades(symbol=symbol, start_time=start_time, end_time=end_time)

    def get_deposit_crypto(self, start_date: int, end_date: int):
        return self.__dao.get_deposit_crypto(start_date=start_date, end_date=end_date)

    def get_withdraw_crypto(self, start_date: int, end_date: int):
        return self.__dao.get_withdraw_crypto(start_date=start_date, end_date=end_date)

    def get_deposit_fiat(self, start_date: int, end_date: int):
        return self.__dao.get_deposit_fiat(start_date=start_date, end_date=end_date)

    def get_withdraw_fiat(self, start_date: int, end_date: int):
        return self.__dao.get_withdraw_fiat(start_date=start_date, end_date=end_date)

    def get_purchase_cx_fiat(self, start_date: int, end_date: int):
        return self.__dao.get_purchase_cx_fiat(start_date=start_date, end_date=end_date)

    def get_sell_cx_fiat(self, start_date: int, end_date: int):
        return self.__dao.get_sell_cx_fiat(start_date=start_date, end_date=end_date)

    def get_price_historical_kline(self, symbol: str, interval: str, start_date: datetime = None,
                                   end_date: datetime = None):
        return self.__dao.get_price_historical_kline(symbol=symbol, interval=interval, start_date=start_date,
                                                     end_date=end_date)

    def get_prev_close_price(self, symbol: str):
        return self.__dao.get_prev_close_price(symbol=symbol)

    def get_actual_price(self, symbol: str):
        return self.__dao.get_actual_price(symbol=symbol)

    def get_symbol_24H(self, symbol: str):
        return self.__dao.get_symbol_24H(symbol=symbol)

    def get_coin_snapshot(self, coin: str):
        return self.__dao.get_coin_snapshot(coin=coin)

    def get_holding_asset(self):
        return self.__dao.get_holding_asset()

    def get_desc_asset_list(self):
        return self.__dao.get_desc_asset_list()

    def get_daily_div_history(self, asset: str = None, limit: int = None):
        return self.__dao.get_daily_div_history(asset=asset, limit=limit)

    def get_fiat_deposit_history(self):
        return self.__dao.get_fiat_deposit_history()

    def get_crypto_to_insert(self) -> list:
        return self.__dao.get_crypto_to_insert()

    def get_buy_sell_fiat_to_insert(self, transaction_type: int, start_time: int, end_time: int) -> list:
        return self.__dao.get_buy_sell_fiat_to_insert(transaction_type=transaction_type, start_time=start_time,
                                                      end_time=end_time)

    def get_deposit_crypto_to_insert(self, start_time: int, end_time: int):
        return self.__dao.get_deposit_crypto_to_insert(start_time=start_time, end_time=end_time)

    def get_deposit_withdraw_fiat_to_insert(self, transaction_type: int, start_time: int, end_time: int):
        return self.__dao.get_deposit_withdraw_fiat_to_insert(transaction_type=transaction_type,
                                                              start_time=start_time, end_time=end_time)

    def get_dividends_to_insert(self, asset: str = None, limit: int = None):
        return self.__dao.get_dividends_to_insert(asset=asset, limit=limit)

    def get_orders_to_insert(self, symbol: str) -> list:
        return self.__dao.get_orders_to_insert(symbol=symbol)

    def get_networks_to_insert(self):
        return self.__dao.get_networks_to_insert()

    def get_symbols_to_insert(self) -> list:
        return self.__dao.get_symbols_to_insert()

    def get_trades_to_insert(self, symbol: str) -> list:
        return self.__dao.get_trades_to_insert(symbol=symbol)

    def get_withdraw_crypto(self, start_time: int, end_time: int):
        return self.__dao.get_withdraw_crypto(start_time=start_time, end_time=end_time)
