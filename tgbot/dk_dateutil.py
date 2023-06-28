import datetime


def get_start_of_month(anyday: datetime.datetime) -> datetime.datetime:
    return anyday.replace(day=1)


def get_finish_of_month(anyday: datetime.datetime) -> datetime.datetime:
    next_month = anyday.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)


def get_start_of_week(anyday: datetime.datetime) -> datetime.datetime:
    return anyday - datetime.timedelta(days=anyday.weekday())


def get_finish_of_week(anyday: datetime.datetime) -> datetime.datetime:
    return get_start_of_week(anyday) + datetime.timedelta(days=6)