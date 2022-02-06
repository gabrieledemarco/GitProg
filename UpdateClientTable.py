from BinanceService import BinanceService
from DbService import DbService
from InsertValueInTable import InsertValueInTable
from SparkDB_table_service import SparkToDBService


class UpdateClientTable:

    def __init__(self):

        self.db = DbService()
        self.spark_ser = SparkToDBService()
        self.users = self.spark_ser.download_users()

    def update_dividends(self):

        update_date = self.db.get_select_with_where(select_columns='update_date', name_table='update_table',
                                                    where_columns='name_table', values_column='dividends')

        for client in self.users:
            ser_bin = BinanceService(api_key=client[1], api_secret=client[2])
            ins_tab = InsertValueInTable(api_key=client[1], api_secret=client[2])


