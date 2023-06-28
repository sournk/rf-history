import pandas as pd
import sqlite3
import pathlib
import unicodedata
import re
import uuid
import datetime

import sys
import os.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from telegram.ext import ContextTypes

import analizer as an
import config
import exceptions
import dk_dateutil as du


def slugify(value, allow_unicode=False):
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')


def load_df_from_file(file_name: str, **kwargs) -> pd.DataFrame:
    df = pd.read_excel(file_name)
    df = an.prepare_columns(df, **kwargs)

    return df


def load_df_from_db(user_id: str, conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql(f'SELECT * FROM {config.TRANS_TABLE_NAME} WHERE USER_ID = :user_id', conn, 
                     params={'user_id': user_id})
    df = an.df_columns_cast(df)
    return df


def save_df_to_db(df: pd.DataFrame, conn: sqlite3.Connection, table_name: str = ''):
    table_name = config.TRANS_TABLE_NAME if table_name == '' else table_name

    df = df.set_index(['USER_ID', 'ORDER_ID'])
    df.to_sql(name=table_name, con=conn, if_exists='append')


async def get_file_from_message(message, context):
    src_file_name = message.document.file_name
    src_file_ext = pathlib.Path(src_file_name).suffix

    if src_file_ext not in ['.xls', '.xlsx']:
        raise Exception('Wrong file type. Send only Excel files.')

    user = message.from_user

    path_to_save = pathlib.Path(f'{config.UPLOAD_FILES_PATH}/{user.id}')
    path_to_save.mkdir(parents=True, exist_ok=True) # create path if not exists

    time_stamp = datetime.datetime.now().strftime('%Y%m%dT%H%M%S')
    user_name = slugify(user.username)
    hash_part = uuid.uuid4().hex

    dst_file_name = str(path_to_save.joinpath(f'{time_stamp}_{user_name}_{hash_part}{src_file_ext}'))
    attached_file = await message.effective_attachment.get_file()
    await attached_file.download_to_drive(custom_path=dst_file_name)

    context.job_queue.run_once(process_uploaded_file, 
                               when=0, 
                               chat_id=message.chat_id, 
                               data={
                                        'FILE_NAME': dst_file_name,
                                        'USER_ID': user.id,
                                        'USER_NAME': user.username,
                                   })


async def process_uploaded_file(context: ContextTypes.DEFAULT_TYPE) -> None:
    """ Load data from uploaded file. Compare data from file with data has already exist in DB and save to DB only new data of user
        
    Args:
        context (ContextTypes.DEFAULT_TYPE): Context of job_queue
    """    
    try:
        job = context.job
        df_file = load_df_from_file(job.data['FILE_NAME'], **job.data)

        main_table_name = config.TRANS_TABLE_NAME
        temp_table_name = f'{config.TRANS_TABLE_NAME}_{uuid.uuid4().hex}'
        save_df_to_db(df=df_file, conn=conn, table_name=temp_table_name)

        query = f'SELECT * FROM {temp_table_name} WHERE ORDER_ID NOT IN (SELECT ORDER_ID FROM {main_table_name} WHERE USER_ID=:user_id)'
        df_new_rows = pd.read_sql(query, con=conn, params={'user_id': job.data['USER_ID']})
        save_df_to_db(df=df_new_rows, conn=conn)

        conn.execute(f'DROP TABLE {temp_table_name};')

        context.job_queue.run_once(get_tech_data_stat, 
                               when=0, 
                               chat_id=job.chat_id,
                               data=job.data)        
    except Exception as e:
        await context.bot.send_message(job.chat_id, text=f"Ошибка при обработке файла {job.data['FILE_NAME']}: {str(e)}")


async def get_tech_data_stat(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        job = context.job
        df = load_df_from_db(job.data['USER_ID'], conn=conn)

        min_date = (df['OPEN_DT'].dt.date).min()
        max_date = (df['OPEN_DT'].dt.date).max()

        await context.bot.send_message(job.chat_id, text=f'Доступен для анализа период с {min_date} по {max_date} {df.shape}')
    except Exception as e:
        await context.bot.send_message(job.chat_id, text=f'Ошибка получения данных: {str(e)}')


async def get_summary(context: ContextTypes.DEFAULT_TYPE) -> None:
    job = context.job
    df_full = load_df_from_db(job.data['USER_ID'], conn=conn)

    interval = job.data['INTERVAL']

    # df_full = an.prepare_columns(df_origin)
    df_orders = an.extend_with_grid_details(df_full)
    df_orders = df_orders.sort_values(by='OPEN_DT', ascending=False)
    df_grids = an.get_grids(df_orders)

    last_open_df_dt = df_full['OPEN_DT'].dt.date.max()

    if interval == 'month':
        start_date = du.get_start_of_month(last_open_df_dt)
        finish_date = du.get_finish_of_month(last_open_df_dt)

    if interval == 'monthprev':
        start_date = du.get_start_of_month(last_open_df_dt)
        last_open_df_dt = start_date - datetime.timedelta(days=1)
        start_date = du.get_start_of_month(last_open_df_dt)
        finish_date = du.get_finish_of_month(last_open_df_dt)

    if interval == 'week':
        start_date = du.get_start_of_week(last_open_df_dt)
        finish_date = du.get_finish_of_week(last_open_df_dt)

    if interval == 'weekprev':
        start_date = du.get_start_of_week(last_open_df_dt)
        last_open_df_dt = start_date - datetime.timedelta(days=1)
        start_date = du.get_start_of_week(last_open_df_dt)
        finish_date = du.get_finish_of_week(last_open_df_dt)


    if interval != 'summary':
        df_full = df_full[(df_full['OPEN_DT'].dt.date >= start_date) & (df_full['OPEN_DT'].dt.date <= finish_date)]
        df_orders = df_orders[(df_orders['OPEN_DT'].dt.date >= start_date) & (df_orders['OPEN_DT'].dt.date <= finish_date)]
        df_grids = df_grids[(df_grids['OPEN_DT'].dt.date >= start_date) & (df_grids['OPEN_DT'].dt.date <= finish_date)]

    df_sum = an.get_summary(df_full=df_full,
                            df_orders=df_orders,
                            df_grids=df_grids)

    summary_answer_dict = {'message001': (f"Период: {df_sum.iloc[0]['START_DATE']} - {df_sum.iloc[0]['FINISH_DATE']}\n"
                                          f"Календарных дней: {df_sum.iloc[0]['CAL_DAYS']:,.0f}\n"
                                          f"Торговых дней: {df_sum.iloc[0]['DAYS']:,.0f}"),
                
                           'message002': (f"Баланс: ${df_sum.iloc[0]['BALANCE']:,.2f}\n"
                                          f"Пополнений: ${df_sum.iloc[0]['DK_DEPOSIT']:,.2f}\n"
                                          f"Снятий: ${df_sum.iloc[0]['DK_WITHDRAWAL']:,.2f}\n"
                                          f"Прочие движения: ${df_sum.iloc[0]['DK_MISC_TRANS']:,.2f}"),
                           'message003': (f"Собственных средств: ${df_sum.iloc[0]['OWN_FUNDS']:,.2f}"),                                          
                           'message004': (f"Прибыль: ${df_sum.iloc[0]['PROFIT']:,.2f}\n"
                                          f"Средняя прибыль в календарный день: ${df_sum.iloc[0]['PROFIT_PER_CAL_DAY']:,.2f}\n"
                                          f"Средняя прибыль в торговый день: ${df_sum.iloc[0]['PROFIT_PER_DAY']:,.2f}"),
                           'message005': (f"ROA: {df_sum.iloc[0]['ROA']*100:,.1f}%\n"
                                          f"ROA календарных дней: {df_sum.iloc[0]['ROA_DAYS']:,.0f}\n"
                                          f"ROI: {df_sum.iloc[0]['ROI']*100:,.1f}%\n"
                                          f"ROI календарных дней: {df_sum.iloc[0]['ROI_DAYS']:,.0f}"),
                           'message006': (f"Ордеров: {df_sum.iloc[0]['ORDER_ID']:,.0f}\n"
                                          f"Прибыльных: {df_sum.iloc[0]['HAS_ORDER_PROFIT']:,.0f}\n"
                                          f"Win Rate: {df_sum.iloc[0]['WIN_RATE']*100:,.1f}%\n"
                                          f"Прибыль ордера: AVG=${df_sum.iloc[0]['AVG_ORDER_PROFIT']:,.2f} | MAX=${df_sum.iloc[0]['MAX_ORDER_PROFIT']:,.2f}\n"
                                          f"Убыток ордера: MAX=${df_sum.iloc[0]['MAX_ORDER_LOSS']:,.2f}"),
                           'message007': (f"Cеток однонаправленных: {df_sum.iloc[0]['GRID_CNT']:,.0f}\n"
                                          f"Лот на $1000 депозита: MIN={df_sum.iloc[0]['MIN_LOT_1000']:,.4f} | AVG={df_sum.iloc[0]['AVG_LOT_1000']:,.4f} | MAX={df_sum.iloc[0]['MAX_LOT_1000']:,.4f} | LAST={df_sum.iloc[0]['LAST_LOT_1000']:,.4f}\n"
                                          f"Ордеров в сетке: AVG={df_sum.iloc[0]['AVG_GRID_ORDER_CNT']:,.1f} | MAX={df_sum.iloc[0]['MAX_GRID_ORDER_CNT']:,.0f}\n"
                                          f"Длительность сетки: MIN={df_sum.iloc[0]['MIN_GRID_DURATION']} | AVG={df_sum.iloc[0]['AVG_GRID_DURATION']} | MAX={df_sum.iloc[0]['MAX_GRID_DURATION']}\n"
                                          f"Прибыль сетки: AVG=${df_sum.iloc[0]['AVG_GRID_PROFIT']:,.2f} | MAX=${df_sum.iloc[0]['MAX_GRID_PROFIT']:,.2f}\n"
                                          f"Просадка сетки: AVG=-${df_sum.iloc[0]['AVG_GRID_DRAWDOWN']:,.2f} | MAX=-${df_sum.iloc[0]['MAX_GRID_DRAWDOWN']:,.2f}\n"
                                          f"Просадка сетки от депозита: AVG={df_sum.iloc[0]['AVG_GRID_DRAWDOWN_RATIO']*100:,.1f}% | MAX={df_sum.iloc[0]['MAX_GRID_DRAWDOWN_RATIO']*100:,.1f}%"),
    }

    for k, v in summary_answer_dict.items():
        await context.bot.send_message(job.chat_id, text=v)

    fig = an.get_summary_chart(df_grids)

    temp_path = pathlib.Path(f"{config.TEMP_FILES_PATH}/{job.data['USER_ID']}")
    temp_path.mkdir(parents=True, exist_ok=True) # create path if not exists

    temp_file_name = str(temp_path.joinpath(f'summary_{uuid.uuid4().hex}.png'))
    fig.savefig(temp_file_name, format='png')
    await context.bot.send_photo(job.chat_id, photo=open(temp_file_name, 'rb'))

    temp_file_name = str(temp_path.joinpath(f'drawdown_{uuid.uuid4().hex}.png'))
    fig = an.get_worst_equity_20_chart(df_grids=df_grids)
    fig.savefig(temp_file_name, format='png')
    await context.bot.send_photo(job.chat_id, photo=open(temp_file_name, 'rb'))
    

    


conn = sqlite3.connect('db.sqlite')
