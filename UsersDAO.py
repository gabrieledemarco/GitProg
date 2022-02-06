from datetime import datetime

from DbService import DbService


class UsersDAO:

    def __init__(self, api_key: str, api_secret: str, nick_name: str):

        self.db_ser = DbService()
        self.api_key = api_key
        self.api_secret = api_secret
        self.nick_name = nick_name

    def insert_user(self):
        return self.db_ser.insert(name_table='users', list_record=[self.api_key, self.api_secret, self.nick_name,
                                                                   datetime.now()])


class UsersService:

    def __init__(self, api_key: str, api_secret: str, nick_name: str):
        self.user_dao = UsersDAO(api_key, api_secret, nick_name)

    def insert_user(self):
        return self.user_dao.insert_user()
