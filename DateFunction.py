from datetime import datetime


def datetime_to_date(input_data):
    return input_data.date()


def datetime_to_milliseconds_int(input_data: datetime):
    return int(input_data.timestamp()) * 1000


def milliseconds_to_datetime(input_data: int):
    return datetime.fromtimestamp(input_data/1000)


def string_to_datetime(input_data, format_date: str):
    return datetime.strptime(input_data, format_date)


def now_date():
    return datetime.now()


def limit(start_date: datetime, end_date: datetime):
    return (datetime_to_date(input_data=end_date) - datetime_to_date(input_data=start_date)).days
