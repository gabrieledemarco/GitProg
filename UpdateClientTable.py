from datetime import datetime

import pyspark.sql.functions as F

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
        new_users = self.users.select("api_key", "api_secret", "registration_date")\
                        .where(F.col("registration_date") > update_date)
        old_users = self.users.select("api_key", "api_secret", "registration_date")\
                        .where(F.col("registration_date") < update_date)

        for new_client in new_users.rdd.collect():
            ser_bin = BinanceService(api_key=new_client[0], api_secret=new_client[1])
            ins_tab = InsertValueInTable(api_key=new_client[0], api_secret=new_client[1])
            limit_div = (datetime.now().date() - update_date.date()).days



