from datetime import datetime

from DbService import DbService
from InsertValueInTable import InsertValueInTable


class UsersDAO:

    def __init__(self, api_key: str, api_secret: str, nick_name: str, pass_word: str):
        self.db_ser = DbService()
        self.api_key = api_key
        self.api_secret = api_secret
        self.nick_name = nick_name
        self.pass_word = pass_word

    def is_user_registered(self) -> bool:
        user = self.db_ser.get_select_with_where(select_columns='nickname',
                                                 name_table='users',
                                                 where_columns='nickname',
                                                 values_column=self.nick_name)
        value = 0
        if len(user) == 0:
            value = False
        elif len(user) > 0:
            value = True
        else:
            print("something goes wrong")

        return value

    def insert_user(self):
        return self.db_ser.insert(name_table='users', list_record=[(self.api_key, self.api_secret,
                                                                              self.nick_name,
                                                                              self.pass_word,
                                                                              datetime.now())])

    def insert_new_user_and_data(self):
        self.insert_user()
        insert_value = InsertValueInTable(api_key=self.api_key, api_secret=self.api_secret)
        insert_value.insert_dividends()
        insert_value.insert_orders()
        insert_value.insert_trades()
        insert_value.insert_deposit_withdraw()



"""
class UsersService:

    def __init__(self, api_key: str, api_secret: str, nick_name: str, pass_word: str):
        self.user_dao = UsersDAO(api_key, api_secret, nick_name, pass_word)
        # self.insert_value = InsertValueInTable(api_key=api_key, api_secret=api_secret)

    def is_user_registered(self) -> bool:
        # Return False is user is Not registered
        return self.is_user_registered()

    def insert_new_user_and_data(self):
        self.user_dao.insert_user()
        # insert_value = InsertValueInTable(api_key=self.user_dao.api_key, api_secret=self.user_dao.api_secret)
        # insert_value.insert_dividends()
        # insert_value.insert_orders()
        # insert_value.insert_trades()
        # insert_value.insert_deposit_withdraw()
"""

