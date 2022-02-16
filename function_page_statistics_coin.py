from datetime import timedelta

import numpy as np
import pandas as pd

import DateFunction as dT
from BinanceService import BinanceService
from CommonTable import CommonTable
from CreateTables import engine_fin

pd.set_option('display.max_columns', None)


def get_df_date(start_date):
    index_date = pd.date_range(start=start_date, end=dT.datetime_to_date(dT.now_date()), freq="D")
    return pd.DataFrame(data=[1] * len(index_date), index=index_date, columns=["Val"])


def min_date(df_list: list):
    return min([min(df.index) for df in df_list if not df.empty])


def wags(d, w):
    return sum(d * w) / sum(w)


class StatisticsCoin:

    def __init__(self, id_user: int, api_key: str, api_secret: str):
        # self.db = DbService()
        self.comm = CommonTable()
        self.df_trade = pd.read_sql(sql=f"select * from trades where id_user={id_user}", con=engine_fin)
        engine_fin.dispose()
        self.df_dep = pd.read_sql(sql=f"select * from deposits_crypto where id_user={id_user}",
                                    con=engine_fin)
        engine_fin.dispose()
        self.df_order = pd.read_sql(sql=f"select * from orders where id_user={id_user}",
                                    con=engine_fin)
        engine_fin.dispose()
        self.df_with = pd.read_sql(sql=f"select * from withdraw_crypto where id_user={id_user}",
                                    con=engine_fin)
        engine_fin.dispose()
        self.df_div = pd.read_sql(sql=f"select * from dividends where id_user={id_user}",
                                    con=engine_fin)
        engine_fin.dispose()
        self.df_buy_sell = pd.read_sql(sql=f"select * from buy_sell_fiat where id_user={id_user}",
                                        con=engine_fin)
        engine_fin.dispose()

        self.df_symbol = pd.read_sql_table(table_name="symbols", con=engine_fin)
        engine_fin.dispose()

        self.bin_ser = BinanceService(api_key=api_key, api_secret=api_secret)

    def get_historical_amount(self, coin: str):
        """ Desc: Return df of Historic Amount invested in a given coin
                Input:
                    coin:str
                Ouput
                    df : ['Dividend']['Trade']['Deposit']['Withdraw']['Sold']['Purchase'][Amount]
                    """
        self.df_trade['time'] = pd.to_datetime(self.df_trade['time']).dt.date
        self.df_trade = self.df_trade.loc[self.df_trade['symbol'].str.startswith(coin),
                                          ['time', 'qty']].set_index('time')\
            .sort_index().rename(index={'time': 'date'})

        self.df_dep['inserttime'] = pd.to_datetime(self.df_dep['inserttime']).dt.date
        self.df_dep = self.df_dep.loc[self.df_dep['coin'] == coin, ['inserttime', 'amount']].set_index('inserttime')\
            .sort_index().rename(columns={'amount': 'amount_dep'}, index={'inserttime': 'date'})

        self.df_with['applytime'] = pd.to_datetime(self.df_with['applytime']).dt.date
        self.df_with = self.df_with.loc[self.df_with['coin'] == coin, ['applytime', 'amount']].set_index('applytime')\
            .sort_index().rename(columns={'amount': 'amount_with'}, index={'applytime': 'date'})

        self.df_div['div_time'] = pd.to_datetime(self.df_div['div_time']).dt.date
        self.df_div = self.df_div.loc[self.df_div['asset'] == coin, ['div_time', 'amount']].set_index('div_time')\
            .sort_index().rename(columns={'amount': 'amount_div'}, index={'div_time': 'date'})

        self.df_buy_sell['updatetime'] = pd.to_datetime(self.df_buy_sell['updatetime']).dt.date
        self.df_buy_sell['obtainamount'] = np.where(self.df_buy_sell['buy_sell'] == "B",
                                                    self.df_buy_sell['obtainamount'],
                                                    - self.df_buy_sell['obtainamount'])
        self.df_buy_sell = self.df_buy_sell.loc[(self.df_buy_sell['cryptocurrency'] == coin) &
                                                (self.df_buy_sell['status'] == "Completed"),
                                                ['updatetime', 'obtainamount']].set_index('updatetime').sort_index()\
            .rename(index={'updatetime': 'date'})

        mini_date = min_date([self.df_trade, self.df_dep, self.df_with, self.df_div, self.df_buy_sell])
        df_date = get_df_date(start_date=mini_date)
        result = df_date.join(self.df_div).join(self.df_trade).join(self.df_dep).join(self.df_with).\
            join(self.df_buy_sell).fillna(0)

        del result['Val']
        result['Amount'] = (result['amount_div'] + result['qty'] +
                            result['amount_dep'] + result['amount_with'] +
                            result['obtainamount']).cumsum()

        return result

    def get_PL_x_traded_symbols(self, coin: str):

        df_tot = pd.merge(self.df_trade.loc[self.df_trade['symbol'].str.startswith(coin),
                                            ['symbol', 'id_order', 'price', 'time', 'commission_asset', 'qty']],
                          self.df_order.loc[self.df_order['symbol'].str.startswith(coin),
                                            ['side', 'symbol', 'id_order_bin']], how="inner",
                          left_on=['symbol', 'id_order'], right_on=['symbol', 'id_order_bin'])

        if not df_tot.empty:

            tot = df_tot.groupby(['symbol', 'side']).agg({'qty': np.sum, 'price': np.mean}).reset_index()
            tot['change'] = tot.groupby('symbol')['price'].pct_change().fillna(0)

            tot['deposit'] = tot['qty'] * tot['price']
            tot['sold_dv'] = tot['price'] * (1 + tot['change']) * tot['qty']
            tot['tot_PL'] = tot['change'] * tot['qty'] * tot['price']
            tot['tot_Pl_perc'] = tot['tot_PL'] / tot['deposit']
            tot.set_index('symbol', inplace=True)

            return tot

    def get_PL_x_trx_symbols(self, coin: str):
        df = self.df_buy_sell.loc[(self.df_buy_sell['cryptocurrency'] == coin) &
                              (self.df_buy_sell['status'] == 'Completed'), ['fiatcurrency', 'buy_sell', 'sourceamount',
                                                                            'price']]

        if not df.empty:
            tot = df.groupby(['fiatcurrency', 'buy_sell']).agg({'sourceamount': np.sum, 'price': np.mean}).reset_index()
            tot['change'] = tot.groupby('fiatcurrency')['price'].pct_change().fillna(0)

            tot['deposit'] = tot['sourceamount'] * tot['price']
            tot['tot_PL'] = tot['change'] * tot['sourceamount'] * tot['price']
            tot['tot_Pl_perc'] = tot['tot_PL'] / tot['deposit']
            tot.set_index('fiatcurrency', inplace=True)

            return tot

    def get_df_Weights_from_trade(self, coin: str, quote: str): # rivedere
        df_tot = pd.merge(self.df_trade.loc[self.df_trade['symbol'].str.startswith(coin),
                                            ['symbol', 'id_order', 'price', 'time', 'commission_asset', 'qty']],
                          self.df_order.loc[self.df_order['symbol'].str.startswith(coin),
                                            ['side', 'symbol', 'id_order_bin']], how="inner",
                          left_on=['symbol', 'id_order'], right_on=['symbol', 'id_order_bin'])

        df_tot['coin'] = coin
        df_tot['quote'] = df_tot['symbol'].str.split(coin).str[-1]
        df_tot['symbol_price'] = np.where((df_tot['quote'] != "USDT") & ((df_tot['symbol'].str.startswith('EUR')) |
                                                                          (df_tot['symbol'].str.startswith('USDT'))),
                                          df_tot['quote'] + df_tot['coin'], df_tot['symbol'])
        price_tot = []
        for i in range(len(df_tot)):
            price_tot.append(self.bin_ser.get_price_historical_kline(symbol=df_tot.loc[i, 'symbol_price'],
                                                                           interval="1d",
                                                                           start_date=df_tot.loc[i, 'time']
                                                                                      - timedelta(days=1),
                                                                           end_date=df_tot.loc[i, 'time']))

        df_tot['conv_price'] = price_tot
        df_tot['new_qty'] = np.where((df_tot['quote'] != "USDT") & ((df_tot['symbol'].str.startswith('EUR')) |
                                                                          (df_tot['symbol'].str.startswith('USDT'))),
                                     df_tot['qty']/df_tot['conv_price'], df_tot['qty']*df_tot['conv_price'])
        print(df_tot[['symbol_price', 'conv_price', 'qty', 'new_qty']])
        df_sum_g = df_tot[['symbol_price', 'new_qty']].groupby('symbol_price').sum()
        print(df_sum_g)

    def get_valid_conversion_Price(self, p_coin_in_quote_0: float, quote_0: str, quote_1: str, time=None) -> float:
        ticker = self.comm.get_valid_ticker(coin=quote_0, quote=quote_1)

        if time is None:
            p_ticker = self.bin_ser.get_actual_price(symbol=ticker)
        else:
            end_date = time + timedelta(days=1)
            p_ticker = self.bin_ser.get_price_historical_kline(symbol=ticker, interval="1d", start_date=time,
                                                               end_date=end_date)

        l_quote_0 = len(quote_0)
        price_in_quote_1 = 0.0

        if ticker[-l_quote_0:] == quote_0:
            price_in_quote_1 = p_coin_in_quote_0 / p_ticker
        elif ticker[0:l_quote_0] == quote_0:
            price_in_quote_1 = p_coin_in_quote_0 * p_ticker
        else:
            print("we")

        return price_in_quote_1

    def conversion(self, coin: str, quote: str):
        df_trade_tot = pd.merge(self.df_trade.loc[self.df_trade['symbol'].str.startswith(coin),
                                                  ['symbol', 'id_order', 'price', 'time', 'commission_asset', 'qty']],
                                self.df_order.loc[self.df_order['symbol'].str.startswith(coin),
                                                  ['side', 'symbol', 'id_order_bin']], how="inner",
                                left_on=['symbol', 'id_order'], right_on=['symbol', 'id_order_bin'])

        df_trx = self.df_buy_sell.loc[(self.df_buy_sell['cryptocurrency'] == coin) &
                                      (self.df_buy_sell['status'] == 'Completed'), ['updatetime', 'fiatcurrency',
                                                                                    'buy_sell', 'obtainamount',
                                                                                    'price']]
        df_trade_tot['qty'] = np.where(df_trade_tot['side'] == "BUY", df_trade_tot['qty'], -df_trade_tot['qty'])
        df_trx['obtainamount'] = np.where(df_trx['buy_sell'] == "B", df_trx['obtainamount'], - df_trx['obtainamount'])

        df_trx = df_trx.rename(columns={"updatetime": "time", "fiatcurrency": "symbol", "buy_sell": "side",
                                        "obtainamount": "qty"})
        df_trx["side"] = df_trx["side"].map({"B": "BUY", "S": "SELL"})
        df_trx['symbol'] = coin + df_trx['symbol']

        result = pd.concat([df_trx, df_trade_tot[['time', 'symbol', 'side', 'qty', 'price']]]).reset_index().\
            drop(["index", "side"], axis=1).sort_values('time')
        result['amount_0T'] = result['qty'].cumsum()

        result['conv_prove'] = np.where(result.symbol.str.replace(coin, "") != quote, result.apply(
            lambda x: self.get_valid_conversion_Price(p_coin_in_quote_0=x['price'],
                                                      quote_0=x['symbol'].replace(coin, ""),
                                                      quote_1=quote, time=x['time']), axis=1), None)

        return result

    def get_df_long_trades_meanPTotAmount_x_coin(self, coin: str):
        symbol_list = self.df_symbol.loc[self.df_symbol['base_asset'] == coin, 'symbol']
        df_trade_tot = pd.merge(self.df_trade.loc[self.df_trade['symbol'].str.startswith(coin),
                                                  ['symbol', 'id_order', 'price', 'time', 'commission_asset', 'qty']],
                                self.df_order.loc[(self.df_order['symbol'].str.startswith(coin)) &
                                                  (self.df_order['side'] == "BUY"),
                                                  ['side', 'symbol', 'id_order_bin']], how="inner",
                                left_on=['symbol', 'id_order'], right_on=['symbol', 'id_order_bin'])

        df_trade_tot = df_trade_tot[['symbol', 'side', 'qty', 'price']]
        mean_weight = df_trade_tot.groupby('symbol').apply(lambda x: wags(x['price'], x['qty'])).to_frame()#\
          #  .rename(columns={"0": "mean_price"}, inplace=True)
        print(mean_weight)
        mean_weight['tot_qty'] = df_trade_tot.groupby('symbol')['qty'].sum()
        print(mean_weight.rename(columns={0: "mean_price"}, inplace=True))


a = StatisticsCoin(id_user=1, api_key="2mYr1HH1a9O3LR3ogAoO9SowRD0DwFX9nLZRUnGifIPmGfmznoVemAqRVc8JKMoC",
                   api_secret="LvUCcMAe3FecFpY9KVQMOquD8UpYHJfCY1y9EbzMgbSCwhHBmB4CruhsBUzKYsa5")
a.get_df_long_trades_meanPTotAmount_x_coin("SOL")


"""sel = f"select distinct t.symbol, t.price, o.side," \
              f" t.qty from trades t join " \
              f" orders o on o.id_order_bin = t.id_order" \
              f" join symbols s on s.symbol = o.symbol " \
              f" where s.symbol = '{symbol}' and " \
              f" o.side = 'BUY' and o.status ='FILLED'"""
