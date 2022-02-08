from Database import Database


class DbDao:
    def __init__(self):
        self.__db = Database()

    def name_columns(self, name_table: str):
        sel = f"select * from public.{name_table} limit 0"
        self.__db.execute(sel)
        name = self.__db.name_columns()
        return name

    def insert(self, name_table: str, name_columns: list, list_record: list):
        records_list_template = ",".join(["%s"] * len(list_record))
        n_col = ','.join(name_columns[1:])
        ins = f"insert into public.{name_table} ({n_col}) values {records_list_template}"
        self.__db.execute_and_commit(ins, list_record)

    def is_not_empty(self, name_table: str) -> bool:
        sel = f"select count(*) from public.{name_table}"
        self.__db.execute(sel)
        row = self.__db.fetchOne()
        return row[0] > 0

    def count_records(self, name_table: str) -> int:
        sel = f"select count(*) from public.{name_table}"
        self.__db.execute(sel)
        row = self.__db.fetchOne()
        return row[0]

    def get_all_value_in_column(self, name_column, name_table) -> list:
        sel = f"select {name_column} from public.{name_table}"
        self.__db.execute(sel)
        rows = self.__db.fetchAll()
        if name_column == "*":
            all_value = [row for row in rows]
        else:
            all_value = [row[0] for row in rows]
        return all_value

    def get_select_with_where(self, select_columns, name_table: str, where_columns, values_column):

        if type(where_columns) == list:
            list_val = [[where_columns[i], f"{values_column[i]}"] if type(values_column[i]) == int
                        else [where_columns[i], f"'{values_column[i]}'"] for i in range(len(values_column))]
            a = " where " + " and ".join([" = ".join(x) for x in list_val])

        else:
            if type(values_column) == int or type(values_column) == bool:
                a = f" where {where_columns} = {values_column}"
            else:
                a = f" where {where_columns} = '{values_column}'"

        if type(select_columns) == list:
            sel_fin = f"select " + ", ".join(select_columns) + f" from public.{name_table}" + a
        else:
            sel_fin = f"select {select_columns} from public.{name_table}" + a

        self.__db.execute(sel_fin)
        rows = self.__db.fetchAll()
        if type(select_columns) == list:
            all_value = [row for row in rows]
        else:
            all_value = [row[0] for row in rows]
        return all_value

    def delete_where_condition(self, name_table: str, where_columns, values_column):
        if type(where_columns) == list:
            list_val = [[where_columns[i], f"{values_column[i]}"] if type(values_column[i]) == int
                        else [where_columns[i], f"'{values_column[i]}'"] for i in range(len(values_column))]
            a = " where " + " and ".join([" = ".join(x) for x in list_val])

        else:
            if type(values_column) == int or type(values_column) == bool:
                a = f" where {where_columns} = {values_column}"
            else:
                a = f" where {where_columns} = '{values_column}'"

        del_str = f"delete from public.{name_table}" + a
        self.__db.execute_and_commit(del_str)

    def delete(self, name_table: str):
        del_str = f"delete from public.{name_table}"
        self.__db.execute_and_commit(del_str)
