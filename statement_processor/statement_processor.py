import sys
import shutil
import pathlib
import uuid
import datetime
from enum import Enum
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3

sys.path.append("..")
import config
from config import statement_dispatcher_logger
import analizer as an


class HTMLStatementType(Enum):
    ROBOFOREX_WEB_STATEMENT = 'RoboForex'
    MT4_STATEMENT = 'RoboMarkets Ltd'
        
COLUMNS_MAPPING_STATEMENT_TEMPLATE={
    HTMLStatementType.MT4_STATEMENT: {
        'Ticket': 'ORDER_ID', 
        'Open Time': 'OPEN_DT', 
        'Type': 'SIDE', 
        'Size': 'QTY', 
        'Item': 'SYMBOL', 
        'Price': 'OPEN_PRICE', 
        'S / L': 'STOP_LOSS', 
        'T / P': 'TAKE_PROFIT',
        'Close Time': 'CLOSE_DT', 
        'Close': 'CLOSE_PRICE',  # In default MT4 template close and open price cols have same name 'Price', That's why here uses 'Close' name, which is forced set after parsing
        'Commission': 'FEE', 
        'Taxes': 'TAXES',
        'Swap': 'SWAP', 
        'Profit': 'PROFIT', 
        'Comment': 'COMMENT',
    },
    HTMLStatementType.ROBOFOREX_WEB_STATEMENT: {
        'Ticket': 'ORDER_ID', 
        'Open Time': 'OPEN_DT', 
        'Type': 'SIDE', 
        'Size': 'QTY', 
        'Item': 'SYMBOL', 
        'Price': 'OPEN_PRICE', 
        'S / L': 'STOP_LOSS', 
        'T / P': 'TAKE_PROFIT',
        'Close Time': 'CLOSE_DT', 
        'Close': 'CLOSE_PRICE',  # In default MT4 template close and open price cols have same name 'Price', That's why here uses 'Close' name, which is forced set after parsing
        'Commission': 'FEE', 
        'R/O Swap': 'SWAP', 
        'Trade P/L': 'PROFIT', 
        'Comment': 'COMMENT',
    },
}

ZERO_COLUMNS_FOR_BALANCE_TRANS_OF_STATEMENT_TEMPLATE = {
    HTMLStatementType.MT4_STATEMENT: ['Size', 'Item', 'Price', 'S / L', 'T / P', 'Close', 'Commission', 'Taxes', 'Swap'],
    HTMLStatementType.ROBOFOREX_WEB_STATEMENT: ['Size', 'Item', 'Price', 'S / L', 'T / P', 'Close', 'Commission', 'R/O Swap'],
}

TYPE_FOR_BALANCE = "balance"
TYPE_FOR_SELL = 'sell'
TYPE_FOR_BUY = 'buy'


class StatementProcessingError(Exception):
    pass

class StatementProcessingWrongFileFormatError(StatementProcessingError):
    pass

class StatementProcessingCopyFileError(StatementProcessingError):
    pass

class StatementProcessingFileAccountIsNotAssignedToUserError(StatementProcessingError):
    pass


def get_file_details(file_name: pathlib.Path) -> dict:
    """
    Parses HTML template and determinants it format - MT4 or Roboforex

    Args:
        file_name (pathlib.Path): file name

    Returns:
        dict: {type: HTMLStatementType, account: <Roboforex account number>, name: <Roboforex account owner name>
    """
    with open(str(file_name), 'r') as f:
        content = f.read()
        soup = BeautifulSoup(content, 'lxml')

        res = {'type': HTMLStatementType(soup.find('b').text), 
                'account': soup.find('td').b.text.replace('Account: ', '')}
        res['name'] = soup.find_all('td')[1].b.text.replace('Name: ', '') \
            if res['type'] == HTMLStatementType.MT4_STATEMENT else ''
            
        return res

def copy_file_from_ftp_to_processing_dir(src: pathlib.Path, dst: pathlib.Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)  # create path if not exists
    shutil.copy2(src=str(src), dst=str(dst))

def read_html_file_as_mt4(file_name: pathlib.Path) -> pd.DataFrame:
    """
    Parses HTML of MT4 default template. 
    Comments for trading trans [buy, sell] is created from scratch and not got from tags. This process is unified with Roboforex template.

    Args:
        file_name (pathlib.Path): file name

    Returns:
        pd.DataFrame: Result DataFrame
    """    
    df_list = pd.read_html(str(file_name))
    df = df_list[0] # Only one table in default statement.htm template
    
    # Get trans comments from <td> title attr and add them to df column
    # You can get trading trans ['buy', 'sell'] original comments from html
    # Use code below
    # with open(str(file_name), 'r') as f:
    #     contents = f.read()
    #     soup = BeautifulSoup(contents, 'lxml')
    #     td_list = soup.find_all('td', title=True)
    #     trans_comments = {tag.string: tag['title'] for tag in td_list}
    #     df['Comment'] = [trans_comments.get(row[0], '') for idx, row in df.iterrows()]
    
    
    start_idx = df[df[0] == 'Ticket'][0].index[0]
    finish_idx = df.iloc[start_idx + 1:][df[0].isna()][0].index[0]
    df = df.iloc[start_idx:finish_idx] 
    df.columns = list(df.iloc[0]) # Move 1st row to cols
    
    # Default MT4 and Roboforex statement template contains 2 cols with same name 'Price'
    # So why we make force change name of last price to 'Close'
    df.columns.values[9] = 'Close' 
    
    # Default MT4 and Roboforex statement template has joined <td> for 'Size' for all type='balance' trans.
    # So why we copy comment from 'Size' to correct col
    df.loc[(df['Type'] == TYPE_FOR_BALANCE), 'Comment'] = df['Size']
    
    # Default MT4 and Roboforex statement template has joined <td> for all type='balance' trans.
    # So why we clear str for all float cols of all type='balance' trans.
    df.loc[(df['Type'] == TYPE_FOR_BALANCE), ZERO_COLUMNS_FOR_BALANCE_TRANS_OF_STATEMENT_TEMPLATE[HTMLStatementType.ROBOFOREX_WEB_STATEMENT]] = 0
    
    # Copy OPEN_DT tot CLOSE_DT for all type='balance' trans.
    df.loc[(df['Type'] == TYPE_FOR_BALANCE), 'Close Time'] = df['Open Time']
    
    return df.iloc[1:]

def read_html_file_as_roboforex(file_name: pathlib.Path) -> pd.DataFrame:
    """
    Parses HTML of Roboforex default template. 
    Comments for trading trans [buy, sell] is created from scratch. This process is unified with Roboforex template.
    Adds 0 valued Taxes col, cuz it not exists in default Roboforex template.

    Args:
        file_name (pathlib.Path): file name

    Returns:
        pd.DataFrame: Result DataFrame
    """        
    df_list = pd.read_html(str(file_name))
    df = df_list[0] # Only one table in default statement.htm template
    
    start_idx = df[df[0] == 'Ticket'][0].index[0]
    finish_idx = df.iloc[start_idx + 1:][df[0].isna()][0].index[0]
    df = df.iloc[start_idx:finish_idx] 
    df.columns = list(df.iloc[0]) # Move 1st row to cols
    
    # Default Roboforex statement template doesn't contain col 'Taxes'.
    # So why we make force add col 'Taxes'
    df['Taxes'] = 0
    
    # Default MT4 and Roboforex statement template contains 2 cols with same name 'Price'
    # So why we make force change name of last price to 'Close'
    df.columns.values[9] = 'Close' 
    
    # Default MT4 and Roboforex statement template has joined <td> for 'Size' for all type='balance' trans.
    # So why we copy comment from 'Size' to correct col
    df['Comment'] = ''
    df.loc[(df['Type'] == TYPE_FOR_BALANCE), 'Comment'] = df['Size']
    
    # Default MT4 and Roboforex statement template has joined <td> for all type='balance' trans.
    # So why we clear str for all float cols of all type='balance' trans.
    df.loc[(df['Type'] == TYPE_FOR_BALANCE), ZERO_COLUMNS_FOR_BALANCE_TRANS_OF_STATEMENT_TEMPLATE[HTMLStatementType.ROBOFOREX_WEB_STATEMENT]] = 0
    
    # Copy OPEN_DT tot CLOSE_DT for all type='balance' trans.
    df.loc[(df['Type'] == TYPE_FOR_BALANCE), 'Close Time'] = df['Open Time']
    
    return df.iloc[1:]

    
def df_columns_cast(df: pd.DataFrame) -> pd.DataFrame:
    # Datetime casting
    df['OPEN_DT'] = pd.to_datetime(df['OPEN_DT'], errors='ignore')
    df['CLOSE_DT'] = pd.to_datetime(df['CLOSE_DT'], errors='ignore')

    return df

def prepare_columns(df: pd.DataFrame, columns_mapping_dict: dict, **kwargs) -> pd.DataFrame:
    """    
    1. Renames columns using COLUMNS_MAPPING. 
    2. Add to DataFrame user meta columns from **kwargs

    Args:
        df (pd.DataFrame): Source DataFrame loaded from file
        columns_mapping_dict (dict): Mapping cols name dict to rename cols

    Raises:
        StatementProcessingError: Raises when source DataFrame doesn't have all necessary cols from columns_mapping_dict

    Returns:
        pd.DataFrame: Result DataFrame
    """    
    
    df_res = df.copy()
    df_res = df_res.rename(columns=columns_mapping_dict)
    if not all([c in df_res.columns for c in columns_mapping_dict.values()]):
        l = [c in df_res.columns for c in columns_mapping_dict.values()]
        miss_columns_list = [list(columns_mapping_dict.values())[idx[0]] for idx in filter(lambda tup: not tup[1], enumerate(l))]
        raise StatementProcessingError(f"There're no columns in DataFrame {miss_columns_list}")
    
    df_res = df_columns_cast(df_res)
    df_res = df_res.sort_values(by=['OPEN_DT', 'ORDER_ID'])

    for k, v in kwargs.items():
        df_res[k] = v
    
    return df_res

def create_comments_for_trading_trans(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates comment texts for every ['buy', 'sell] transactions based on a next template:
        - Start SELL
        - SELL 2
        - Start BUY
        - SELL 3
        - BUY 2

    Args:
        df (pd.DataFrame): Source DataFrame

    Returns:
        pd.DataFrame: Result DataFrame
    """    
    df = df.copy()
    df = df.sort_values(by=['CLOSE_DT', 'SIDE', 'ORDER_ID'])

    df_grouped_dt = df.groupby('CLOSE_DT').count()
    for dt in df_grouped_dt.index:
        for side in [TYPE_FOR_BUY, TYPE_FOR_SELL]:
            group_orders_cnt = len(df[(df['CLOSE_DT'] == dt) & (df['SIDE'] == side)])
            comments_list = [f'Start {side.upper()}'] + [f'{side.upper()} {num}' for num in range(2, group_orders_cnt + 1)]
            df.loc[(df['CLOSE_DT'] == dt) & (df['SIDE'] == side), 'COMMENT'] = comments_list 
            
    return df  

def save_df_to_db(df: pd.DataFrame, conn: sqlite3.Connection, table_name: str = ''):
    """Saves DataFrame to DB in given table

    Args:
        df (pd.DataFrame): DataFrame to save
        conn (sqlite3.Connection): connection
        table_name (str, optional): Table name. Defaults to '' means config.TRANS_TABLE_NAME
    """    
    table_name = config.TRANS_TABLE_NAME if table_name == '' else table_name

    df = df.set_index(['FTP_USER_ID', 'ORDER_ID'])
    df.to_sql(name=table_name, con=conn, if_exists='append')

def add_new_statement_data_to_db(df: pd.DataFrame, conn: sqlite3.Connection) -> tuple[int, int]:
    """
    Determinants only new trans from df DataFrame, which haven't already stored in DB.

    Args:
        df (pd.DataFrame): DataFrame to save
        conn (sqlite3.Connection): Connection

    Returns:
        tuple[int, int]: Shape of DataFrame with only new trans from given DataFrame that have been saved in DB. Show how many new trans saved to DB
    """    
    ftp_user_id = df.iloc[0]['FTP_USER_ID']
    
    main_table_name = config.TRANS_TABLE_NAME
    temp_table_name = f'{config.TRANS_TABLE_NAME}_{uuid.uuid4().hex}'
    save_df_to_db(df=df, conn=conn, table_name=temp_table_name)

    try:
        query = f'SELECT * FROM {temp_table_name} WHERE ORDER_ID NOT IN (SELECT ORDER_ID FROM {main_table_name} WHERE FTP_USER_ID=:ftp_user_id)'
        df = pd.read_sql(query, con=conn, 
                            params={'ftp_user_id': ftp_user_id})
    except:
        pass
    
    save_df_to_db(df=df, conn=conn)

    conn.execute(f'DROP TABLE {temp_table_name};')        
    return df.shape


def _process_statement_file(
        telegram_user_id: str, 
        ftp_user_id: str,
        src_file_name: str,
        dst_processing_dir: str,
        conn: sqlite3.Connection) -> None:
    """
    Process workflow to save to DB of trans from given statement file:
        1. Copy src file to processing dir
        2. Get template type and details
        3. Parse template by a function designed for template type
        4. Rename and add new meta cols to standardized DataFrame.
        5. Create comments for only trading trans.
        6. Save new trans from DataFrame to DB. 

    Args:
        telegram_user_id (str): _description_
        ftp_user_id (str): _description_
        src_file_name (str): _description_
        dst_processing_dir (str): _description_
        conn (sqlite3.Connection): _description_

    Returns:
        _type_: _description_
    """
    src_file_name = pathlib.Path(src_file_name)

    dst_path = pathlib.Path(f'{dst_processing_dir}/{ftp_user_id}')
    time_stamp = datetime.datetime.now().strftime('%Y%m%dT%H%M%S')
    uuid_file = uuid.uuid4().hex
    dst_file_name = dst_path.joinpath(f'{time_stamp}_{uuid_file}_{src_file_name.name}')

    copy_file_from_ftp_to_processing_dir(src=src_file_name, dst=dst_file_name) 
    statement_dispatcher_logger.debug(f'File successfully copied from {str(src_file_name)} to {str(dst_file_name)}')
    
    file_details = get_file_details(dst_file_name)
    statement_dispatcher_logger.debug(f'File type is {file_details["type"]}. Account {file_details["account"]} data in file {str(dst_file_name.name)}')
    
    # Check that file Account belongs to current user
    try:
        cur = conn.cursor()
        res = cur.execute("SELECT ACCOUNT_NUMBER FROM ACCOUNTS WHERE (FTP_USER_ID = ?) AND (ACCOUNT_NUMBER = ?)", (ftp_user_id, file_details['account']))
        if res.fetchone()[0] != file_details['account']:
            raise StatementProcessingFileAccountIsNotAssignedToUserError(f"File account {file_details['account']} is not assigned to user {ftp_user_id=}")
    except:
            raise StatementProcessingFileAccountIsNotAssignedToUserError(f"File account {file_details['account']} is not assigned to user {ftp_user_id=}")
        
    
    df_file = PARSE_FUNCTION_FOR_STATEMENT_TEMPLATE[file_details['type']](dst_file_name)
    statement_dispatcher_logger.debug(f'File successfully parsed to DataFrame {df_file.shape} {str(dst_file_name.name)}')
        
    df_file = prepare_columns(df=df_file, 
                              columns_mapping_dict=COLUMNS_MAPPING_STATEMENT_TEMPLATE[file_details['type']],
                              **{'FTP_USER_ID': ftp_user_id,
                               'FILE_NAME': str(dst_file_name.name),
                               'CREATED_DT': datetime.datetime.now()})
    statement_dispatcher_logger.debug(f'Statement DataFrame successfully prepared {df_file.shape}')
    
    # Comments recovery
    df_file = create_comments_for_trading_trans(df_file) 
    statement_dispatcher_logger.debug(f'Comments generated successfully {df_file.shape}')
    
    save_df_shape = add_new_statement_data_to_db(df=df_file, conn=conn)
    statement_dispatcher_logger.debug(f'New data from statement DataFrame successfully saved to DB {save_df_shape}')
    
    # save_df_to_db()
    # update_dt()
    # post_user_notification()
    return df_file

def process_statement_file(
        telegram_user_id: str, 
        ftp_user_id: str,
        src_file_name: str,
        dst_processing_dir: str,
        conn: sqlite3.Connection) -> None:
    
    try:
        statement_dispatcher_logger.info(f'Start statement processing for {telegram_user_id=} {ftp_user_id=}')
        df = _process_statement_file(telegram_user_id=telegram_user_id,
                                ftp_user_id=ftp_user_id,
                                src_file_name=src_file_name,
                                dst_processing_dir=dst_processing_dir,
                                conn=conn)
        statement_dispatcher_logger.info(f'Finish statement processing {df.shape}')
        return df
    except Exception as e:
        raise StatementProcessingError(e)
        

PARSE_FUNCTION_FOR_STATEMENT_TEMPLATE = {
    HTMLStatementType.MT4_STATEMENT: read_html_file_as_mt4,
    HTMLStatementType.ROBOFOREX_WEB_STATEMENT: read_html_file_as_roboforex,
}