from datetime import datetime

from DbService import DbService


class UsersDAO:

    def __init__(self, api_key: str, api_secret: str, nick_name: str, pass_word: str):

        self.db_ser = DbService()
        self.api_key = api_key
        self.api_secret = api_secret
        self.nick_name = nick_name
        self.pass_word = pass_word

    def insert_user(self):
        return self.db_ser.insert_one_record(name_table='users', list_record=[self.api_key, self.api_secret,
                                                                              self.nick_name,
                                                                              self.pass_word,
                                                                              datetime.now()])


class UsersService:

    def __init__(self, api_key: str, api_secret: str, nick_name: str, pass_word: str):
        self.user_dao = UsersDAO(api_key, api_secret, nick_name, pass_word)
        # self.insert_value = InsertValueInTable(api_key=api_key, api_secret=api_secret)

    def insert_new_user_and_data(self):

        self.user_dao.insert_user()
        # insert_value = InsertValueInTable(api_key=self.user_dao.api_key, api_secret=self.user_dao.api_secret)
        # insert_value.insert_dividends()
        # insert_value.insert_orders()
        # insert_value.insert_trades()
        # insert_value.insert_deposit_withdraw()


a = UsersDAO(api_key="aaaa", api_secret="aaas", nick_name="ttttt", pass_word="eeeeee")
a.insert_user()
