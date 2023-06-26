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
    return df


def save_df_to_db(df: pd.DataFrame, conn: sqlite3.Connection):
    df = df.set_index(['USER_ID', 'ORDER_ID'])
    df.to_sql(name=config.TRANS_TABLE_NAME, con=conn, if_exists='replace')


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
    job = context.job
    df = load_df_from_file(job.data['FILE_NAME'], **job.data)
    save_df_to_db(df=df, 
                  conn=conn)

    await context.bot.send_message(job.chat_id, text=f"Beep! {df.shape=}")


conn = sqlite3.connect('db.sqlite')
